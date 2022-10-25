import logging
import sys
import pytest
import threading
import datetime
import json

sys.path.append('../keyrock_ob_aggregator')

from decimal import Decimal
from order_book import OrderBook
from exchanges.bitstamp import BitstampWS
from const import LAST_UPDATED_TS, BITSTAMP, BIDS, ASKS


@pytest.fixture
def bitstamp_client():
    ob = {BITSTAMP: OrderBook(), LAST_UPDATED_TS: datetime.datetime.now()}
    lock = threading.Lock()
    logger = logging.getLogger("Test Logger")
    binance_client = BitstampWS("BTC", "USDT", ob, lock, logger)
    return binance_client


def test_ws_subscription_payload(bitstamp_client):
    """
    # Test WS endpoint formation
    """
    payload_dict = json.loads(bitstamp_client._subscription_payload())
    expected_dict = {
          "event": "bts:subscribe",
          "data": {
            "channel": f"order_book_btcusdt"
          }
        }
    assert payload_dict == expected_dict


def test_ob(bitstamp_client):
    """
    We test order book updates and automatic ordering
    """
    data1 = {"data": {'bids': [('19442', '0.0534'), ('19666', '0.2'), ('19555', '1')],
             'asks': [('19678', '0.7'), ('19667', '0.88'), ('19700', '1')]},
             "event": "data"}

    bitstamp_client._on_message(data1, json.dumps(data1))

    # Test bids
    assert bitstamp_client.orderbook[BITSTAMP][BIDS].index(0) == (Decimal('19666'), Decimal('0.2'))
    assert bitstamp_client.orderbook[BITSTAMP][BIDS].index(1) == (Decimal('19555'), Decimal('1'))
    assert bitstamp_client.orderbook[BITSTAMP][BIDS].index(2) == (Decimal('19442'), Decimal('0.0534'))

    # Test asks
    assert bitstamp_client.orderbook[BITSTAMP][ASKS].index(0) == (Decimal('19667'), Decimal('0.88'))
    assert bitstamp_client.orderbook[BITSTAMP][ASKS].index(1) == (Decimal('19678'), Decimal('0.7'))
    assert bitstamp_client.orderbook[BITSTAMP][ASKS].index(2) == (Decimal('19700'), Decimal('1'))

