import requests
import json
import logging
import threading
import backoff

from decimal import Decimal
from typing import Dict, Any, Tuple
from datetime import datetime
from config import BINANCE_WS_ENDPOINT, BINANCE_SNAPSHOT_ENDPOINT
from const import LAST_UPDATED_TS, BINANCE, BIDS, ASKS
from exchanges.ws_client import WSClient


class BinanceWS(WSClient):
    def __init__(self, base_asset: str, quote_asset: str, orderbook: Dict[Any, Any], lock: threading.Lock
                 , logger: logging.Logger):

        logger.info(f"Initializing Binance Feed...")

        # Initialize parent class for Websocket Management
        super().__init__(
            endpoint=self._generate_ws_endpoint(base_asset, quote_asset),
            exchange_name="Binance",
            logger=logger)

        # Initialize local variables
        self._pair = base_asset.upper() + quote_asset.upper()
        self.orderbook = orderbook
        self._lock = lock
        self._initial_update = True
        self._last_updated_id = 0

    @staticmethod
    def _generate_ws_endpoint(base_asset: str, quote_asset: str) -> str:
        """
        Retrieve the Binance Endpoint
        """
        return f"{BINANCE_WS_ENDPOINT}/ws/{base_asset.lower()}{quote_asset.lower()}@depth@100ms"

    def _on_message(self, wsapi, message) -> None:
        """
        How to manage a local order book correctly
        Open a stream to wss://stream.binance.com:9443/ws/bnbbtc@depth.

        1. Buffer the events you receive from the stream.
        2. Get a depth snapshot from https://api.binance.com/api/v3/depth?symbol=BNBBTC&limit=1000 .
        3. Drop any event where u is <= lastUpdateId in the snapshot.
        4. The first processed event should have U <= lastUpdateId+1 AND u >= lastUpdateId+1.
        5. While listening to the stream, each new event's U should be equal to the previous event's u+1.
        6. The data in each event is the absolute quantity for a price level.
        7. If the quantity is 0, remove the price level.
        8. Receiving an event that removes a price level that is not in your local order book can happen and is normal.
        """
        ob_payload = json.loads(message)

        # Depth snapshot if OB is empty
        if not self.orderbook[BINANCE]:
            snapshot = self._fetch_ob_snapshot()
            self._last_updated_id = snapshot['lastUpdateId']
            self.orderbook[BINANCE][BIDS] = {Decimal(price): Decimal(size) for price, size in snapshot[BIDS]}
            self.orderbook[BINANCE][ASKS] = {Decimal(price): Decimal(size) for price, size in snapshot[ASKS]}

        if ob_payload['U'] <= self._last_updated_id+1 <= ob_payload['u']:
            self._last_updated_id = ob_payload['u']
            self.process_updates(ob_payload)
        elif ob_payload['U'] == self._last_updated_id+1:
            self._last_updated_id = ob_payload['u']
            self.process_updates(ob_payload)
        else:
            if self._initial_update:
                # Initial update is expected to be out of sync
                self._initial_update = False
            else:
                self._logger.error(f"Binance Order Book out of sync...")

    def process_updates(self, data: Dict[Any, Any]) -> None:
        """
        Apply bids and asks updates. Update last updated timestamp
        """
        with self._lock:
            # Process bid updates
            for update in data['b']:
                self.update_orderbook(BIDS, update)

            # Process ask updates
            for update in data['a']:
                self.update_orderbook(ASKS, update)

            # Set the last update time
            self.orderbook[LAST_UPDATED_TS] = datetime.now()

    def update_orderbook(self, side: str, update: Tuple[Any, Any]) -> None:
        """
        If size == 0 -> Remove level
        If size > 0 -> Insert/Overwrite Level
        """
        price = Decimal(update[0])
        size = Decimal(update[1])

        if size == 0:
            try:
                del self.orderbook[BINANCE][side][price]
            except KeyError:
                pass
        else:
            self.orderbook[BINANCE][side][price] = size

    @backoff.on_exception(backoff.expo,
                          requests.exceptions.RequestException,
                          max_tries=3,
                          jitter=None)
    def _fetch_ob_snapshot(self) -> Dict[Any, Any]:
        """
        Fetch OB snapshot. Retry 3 times on error
        """
        r = requests.get(f'{BINANCE_SNAPSHOT_ENDPOINT}?symbol={self._pair}&limit=100')
        try:
            r.raise_for_status()
        except Exception as e:
            self._logger.error(f"Error with Binance snapshot fetching: {e}")
            raise e
        return json.loads(r.content.decode())
