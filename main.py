from concurrent import futures
import logging
import time
import click

import grpc
import keyrock_ob_aggregator_pb2_grpc
import keyrock_ob_aggregator_pb2 as orderbook__aggregator__pb2


class OrderbookAggregatorServicer(keyrock_ob_aggregator_pb2_grpc.OrderbookAggregatorServicer):
    def __init__(self, pair: str, ignore_dust: float, logger: logging.Logger):
        self._pair = pair
        self._ignore_dust = ignore_dust
        self._logger = logger

    def get_agg_ob(self):
        level1 = orderbook__aggregator__pb2.Level(exchange="Binance", price=1002, amount=1)
        level2 = orderbook__aggregator__pb2.Level(exchange="Bitstamp", price=1000, amount=1)
        return orderbook__aggregator__pb2.Summary(spread=0.1, bids=[level1], asks=[level2])

    def BookSummary(self, request, context):
        while True:
            time.sleep(1)
            yield self.get_agg_ob()


@click.command()
@click.option('--pair', type=str, default="BTCUSDT")
@click.option('--ignore_dust', default=None)
@click.option('--port', type=int, default=50051)
def main(pair, ignore_dust, port):
    # Initialize logging
    logger = logging.getLogger("Order book Aggregator")

    logger.info(f"Initializing service...")
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    servicer = OrderbookAggregatorServicer(pair=pair, ignore_dust=ignore_dust, logger=logger)
    keyrock_ob_aggregator_pb2_grpc.add_OrderbookAggregatorServicer_to_server(servicer, server)
    server.add_insecure_port(f'[::]:{port}')
    server.start()
    server.wait_for_termination()


if __name__ == '__main__':
    main()
