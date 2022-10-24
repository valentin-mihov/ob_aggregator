from concurrent import futures
from binance import BinanceWS
from const import BINANCE, BITSTAMP, LAST_UPDATED_TS
from datetime import datetime
from threading import Lock
from decimal import Decimal
import logging
import click

import grpc
import keyrock_ob_aggregator_pb2_grpc
import keyrock_ob_aggregator_pb2 as orderbook__aggregator__pb2


class OrderbookAggregatorServicer(keyrock_ob_aggregator_pb2_grpc.OrderbookAggregatorServicer):
    def __init__(self, logger: logging.Logger, orderbook):
        # Store parameter variables
        self._logger = logger
        self._last_transmission = datetime.now()

        # Initialize orderbook and lock
        self.orderbook = orderbook

    def get_agg_ob(self):
        bids = [orderbook__aggregator__pb2.Level(exchange="Binance", price=Decimal(x[0]), amount=Decimal(x[1])) for x in
                self.orderbook[BINANCE]['bids']][:10]
        asks = [orderbook__aggregator__pb2.Level(exchange="Binance", price=Decimal(x[0]), amount=Decimal(x[1])) for x in
                self.orderbook[BINANCE]['asks']][:10]
        spread = bids[-1].price - asks[0].price
        return orderbook__aggregator__pb2.Summary(spread=spread, bids=bids, asks=asks)

    def BookSummary(self, request, context):
        while True:
            if self.orderbook[LAST_UPDATED_TS] > self._last_transmission:
                self._last_transmission = datetime.now()
                yield self.get_agg_ob()


@click.command()
@click.option("--base_asset", type=str, default="BTC")
@click.option('--quote_asset', type=str, default="USDT")
@click.option('--port', type=int, default=50051)
def main(base_asset, quote_asset, port):
    # Initialize logging
    logger = logging.getLogger("Order book Aggregator")

    logger.info(f"Initializing service...")
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))

    # Initialize order book and lock
    orderbook = {BINANCE: {}, BITSTAMP: {}, LAST_UPDATED_TS: datetime.now()}
    lock = Lock()

    # Initialize Binance Exchange
    binance = BinanceWS(base_asset=base_asset, quote_asset=quote_asset, orderbook=orderbook,
                        lock=lock, logger=logger)
    binance.start()

    # Initialize Bitstamp Exchange

    servicer = OrderbookAggregatorServicer( logger=logger, orderbook=orderbook)
    keyrock_ob_aggregator_pb2_grpc.add_OrderbookAggregatorServicer_to_server(servicer, server)
    server.add_insecure_port(f'[::]:{port}')
    server.start()
    server.wait_for_termination()


if __name__ == '__main__':
    main()
