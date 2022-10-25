import logging
import sys
import pytest
import threading
import datetime

sys.path.append('../keyrock_ob_aggregator')

from decimal import Decimal
from order_book import OrderBook
from exchanges.binance import BinanceWS
from const import LAST_UPDATED_TS, BINANCE, BIDS, ASKS


@pytest.fixture
def binance_client():
    ob = {BINANCE: OrderBook(), LAST_UPDATED_TS: datetime.datetime.now()}
    lock = threading.Lock()
    logger = logging.getLogger("Test Logger")
    binance_client = BinanceWS("BTC", "USDT", ob, lock, logger)
    return binance_client


def test_ws_endpoint_formation(binance_client):
    """
    # Test WS endpoint formation
    """
    assert binance_client._generate_ws_endpoint("BTC", "USDT") == 'wss://stream.binance.com:9443/ws/btcusdt@depth'


def test_ob_update(binance_client):
    """
    We test order book updates and automatic ordering
    """
    data1 = {'b': [('19442', '0.0534'), ('19666', '0.2'), ('19555', '1')],
             'a': [('19678', '0.7'), ('19667', '0.88'), ('19700', '1')]}
    binance_client.process_updates(data1)

    # Test bids
    assert binance_client.orderbook[BINANCE][BIDS].index(0) == (Decimal('19666'), Decimal('0.2'))
    assert binance_client.orderbook[BINANCE][BIDS].index(1) == (Decimal('19555'), Decimal('1'))
    assert binance_client.orderbook[BINANCE][BIDS].index(2) == (Decimal('19442'), Decimal('0.0534'))

    # Test asks
    assert binance_client.orderbook[BINANCE][ASKS].index(0) == (Decimal('19667'), Decimal('0.88'))
    assert binance_client.orderbook[BINANCE][ASKS].index(1) == (Decimal('19678'), Decimal('0.7'))
    assert binance_client.orderbook[BINANCE][ASKS].index(2) == (Decimal('19700'), Decimal('1'))


def test_ob_update_with_removal(binance_client):
    """
    Test order book updates with level removal
    """
    data1 = {'b': [('19442', '0.0534'), ('19666', '0.2'), ('19555', '1')],
             'a': [('19678', '0.7'), ('19667', '0.88'), ('19700', '1')]}
    data2 = {'b': [('19666', '0.0000'), ('19555', '0.0000')], 'a': [('19667', '0.0000'), ('19678', '0.0000')]}
    binance_client.process_updates(data1)
    binance_client.process_updates(data2)

    # Test bids
    assert binance_client.orderbook[BINANCE][BIDS].index(0) == (Decimal('19442'), Decimal('0.0534'))

    # Test asks
    assert binance_client.orderbook[BINANCE][ASKS].index(0) == (Decimal('19700'), Decimal('1'))
