import grpc
import click
import keyrock_ob_aggregator_pb2
import keyrock_ob_aggregator_pb2_grpc
from google.protobuf.json_format import MessageToDict
from rich.console import Console
from rich.table import Table
from rich.live import Live


def generate_table(data):
    table = Table(title="\nAggregated Orderbook")
    table.add_column("SIDE")
    table.add_column("PRICE")
    table.add_column("SIZE")
    table.add_column("EXCHANGE")

    if data:
        for ask in data['asks'][::-1]:  # red
            table.add_row("ASK", str(ask['price']), str(ask['amount']), ask['exchange'], style='red')
        table.add_row()

        table.add_row("SPREAD", str(round(data['spread'], 5)), style='blue')

        table.add_row()
        for bid in data['bids']:  # green
            table.add_row("BID", str(bid['price']), str(bid['amount']), bid['exchange'], style='green')

    return table


@click.command()
@click.option("--port", type=int, default=50052)
def run_client(port):
    """
    Simple client that listens to messages from the server
    and updates a TUI table in live mode.
    """
    channel = grpc.insecure_channel(f'localhost:{port}')
    stub = keyrock_ob_aggregator_pb2_grpc.OrderbookAggregatorStub(channel)
    empty = keyrock_ob_aggregator_pb2.Empty()

    try:
        with Live(generate_table([]), refresh_per_second=20) as live:
            for data in stub.BookSummary(empty):
                live.update(generate_table(MessageToDict(data)))
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    run_client()

