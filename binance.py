import requests
import json
import logging
import threading
import time
import backoff

from typing import Dict, Any, Tuple
from datetime import datetime
from config import BINANCE_WS_ENDPOINT, BINANCE_SNAPSHOT_ENDPOINT
from const import LAST_UPDATED_TS, BINANCE
from ws_client import WSClient


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

    @staticmethod
    def _generate_ws_endpoint(base_asset: str, quote_asset: str) -> str:
        """
        Retrieve the Binance Endpoint
        """
        return f"{BINANCE_WS_ENDPOINT}/ws/{base_asset.lower()}{quote_asset.lower()}@depth"

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
            for key, value in self._fetch_ob_snapshot().items():
                self.orderbook[BINANCE][key] = value

        lastUpdateId = self.orderbook[BINANCE]['lastUpdateId']

        self._logger.debug(f"Last update ID: {lastUpdateId} || U: {ob_payload['U']} || u: {ob_payload['u']}")

        if ob_payload['U'] <= lastUpdateId+1 <= ob_payload['u']:
            self.orderbook[BINANCE]['lastUpdateId'] = ob_payload['u']
            self.process_updates(ob_payload)
        elif ob_payload['U'] == lastUpdateId+1:
            self.orderbook[BINANCE]['lastUpdateId'] = ob_payload['u']
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
                self.update_orderbook('bids', update)

            # Process ask updates
            for update in data['a']:
                self.update_orderbook('asks', update)

            # Set the last update time
            self.orderbook[LAST_UPDATED_TS] = datetime.now()

    def update_orderbook(self, side: str, update: Tuple[Any, Any]) -> None:
        """
        If size == 0 -> Remove level
        If size > 0 -> Insert/Overwrite Level
        """
        price, size = update

        for x in range(0, len(self.orderbook[BINANCE][side])):
            if price == self.orderbook[BINANCE][side][x][0]:
                if size == 0:
                    del self.orderbook[BINANCE][side]
                    break
                else:
                    self.orderbook[BINANCE][side][x] = update
                    break
            elif ((price < self.orderbook[BINANCE][side][x][0] and side == 'asks') or
                    (price > self.orderbook[BINANCE][side][x][0] and side == 'bids')):
                if float(size) > 0:
                    self.orderbook[BINANCE][side].insert(x, update)
                    break
                else:
                    break

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


if __name__ == '__main__':
    orderbook = {BINANCE: {}, LAST_UPDATED_TS: datetime.now()}
    lock = threading.Lock()
    binance = BinanceWS("BTC", "USDT", orderbook, lock, logging.getLogger('Binance'))
    binance.run()
    while True:
        with lock:
            print(binance.orderbook)
        time.sleep(1)
