from concurrent import futures
from exchanges.binance import BinanceWS
from exchanges.bitstamp import BitstampWS
from const import BINANCE, BITSTAMP, LAST_UPDATED_TS, BIDS, ASKS
from datetime import datetime
from threading import Lock
from order_book import OrderBook

import logging
import click
import grpc
import keyrock_ob_aggregator_pb2_grpc
import keyrock_ob_aggregator_pb2

logging.basicConfig(format='%(asctime)s %(message)s')


class OrderbookAggregatorServicer(keyrock_ob_aggregator_pb2_grpc.OrderbookAggregatorServicer):
    def __init__(self, logger: logging.Logger, orderbook, levels):
        # Store parameter variables
        self._levels = levels
        self._logger = logger
        self._last_transmission = datetime.now()

        # Initialize orderbook and lock
        self.orderbook = orderbook

    def parse_ob(self, exchange: str, side: str):
        """
        Convert the order book side into a list of gRPC objects.
        Limit the number to the defined number of levels.
        """
        return [keyrock_ob_aggregator_pb2.Level(exchange=exchange, price=p, amount=a) for p, a in self.orderbook[exchange][side].to_list()[:self._levels*10]]

    def get_agg_ob(self):
        # Combine top 10 bids and asks from each exchange
        bids = self.parse_ob(BINANCE, BIDS) + self.parse_ob(BITSTAMP, BIDS)
        asks = self.parse_ob(BINANCE, ASKS) + self.parse_ob(BITSTAMP, ASKS)

        # Then sort desc for bids and asc for asks
        sorted_bids = sorted(bids, key=lambda level: level.price, reverse=True)
        sorted_asks = sorted(asks, key=lambda level: level.price, reverse=False)

        # Calculate Spread
        spread = sorted_asks[0].price - sorted_bids[0].price

        # Return the summary object
        return keyrock_ob_aggregator_pb2.Summary(spread=spread, bids=sorted_bids[:self._levels], asks=sorted_asks[:self._levels])

    def BookSummary(self, request, context):
        """
        We send data only if any of the underlying order books have new updates
        """
        while True:
            if self.orderbook[LAST_UPDATED_TS] > self._last_transmission:
                self._last_transmission = datetime.now()
                yield self.get_agg_ob()


@click.command()
@click.option("--base_asset", type=str, default="BTC")
@click.option('--quote_asset', type=str, default="USDT")
@click.option('--levels', type=int, default=10)
@click.option('--port', type=int, default=50052)
def main(base_asset, quote_asset, levels, port):
    # Initialize logging
    logger = logging.getLogger("Order book Aggregator")

    logger.info(f"Initializing service...")
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))

    # Initialize order book and lock
    orderbook = {BINANCE: OrderBook(), BITSTAMP: OrderBook(), LAST_UPDATED_TS: datetime.now()}
    lock = Lock()

    # Initialize Binance Exchange
    binance = BinanceWS(base_asset=base_asset, quote_asset=quote_asset, orderbook=orderbook,
                        lock=lock, logger=logger)
    binance.daemon = True

    # Initialize Bitstamp Exchange
    bitstamp = BitstampWS(base_asset=base_asset, quote_asset=quote_asset, orderbook=orderbook,
                          lock=lock, logger=logger)
    bitstamp.daemon = True


    # Initialize the gRPC Servicer
    servicer = OrderbookAggregatorServicer(logger=logger, orderbook=orderbook, levels=levels)
    keyrock_ob_aggregator_pb2_grpc.add_OrderbookAggregatorServicer_to_server(servicer, server)
    server.add_insecure_port(f'[::]:{port}')

    # Start the server
    try:
        binance.start()
        server.start()
        bitstamp.start()
        server.wait_for_termination()
    except KeyboardInterrupt:
        logger.info(f"Stopping service...")
        server.stop(grace=False)
        exit()


if __name__ == '__main__':
    main()
