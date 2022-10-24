import json
import logging
import threading

from decimal import Decimal
from order_book import OrderBook
from typing import Dict, Any
from datetime import datetime
from config import BITSTAMP_ENDPOINT
from const import LAST_UPDATED_TS, BITSTAMP, BIDS, ASKS
from exchanges.ws_client import WSClient


class BitstampWS(WSClient):
    def __init__(self, base_asset: str, quote_asset: str, orderbook: Dict[Any, Any], lock: threading.Lock
                 , logger: logging.Logger):

        logger.info(f"Initializing Bitstamp Feed...")

        # Initialize parent class for Websocket Management
        super().__init__(
            endpoint=BITSTAMP_ENDPOINT,
            exchange_name="Bitstamp",
            logger=logger)

        # Initialize local variables
        self._pair = base_asset.upper() + quote_asset.upper()
        self.orderbook = orderbook
        self._lock = lock
        self._initial_update = True

    def _subscription_payload(self):
        p = {
          "event": "bts:subscribe",
          "data": {
            "channel": f"order_book_{self._pair.lower()}"
          }
        }
        return json.dumps(p)

    def _on_open(self, wsapi):
        self._logger.info(f"Connected to {self._exchange_name}")

        # Send the initial subscription payload
        self._ws.send(self._subscription_payload())

    def _on_message(self, wsapi, message) -> None:
        """
        Bitstamp only supports order book snapshots. Thus, we take the payload and overwrite
        the previous bids and asks in this case.
        """
        ob_payload = json.loads(message)
        if ob_payload['event'] == "data":
            with self._lock:
                for side in [BIDS, ASKS]:
                    self.orderbook[BITSTAMP][side] = {Decimal(price): Decimal(size) for price, size in ob_payload['data'][side]}
                self.orderbook[LAST_UPDATED_TS] = datetime.now()
