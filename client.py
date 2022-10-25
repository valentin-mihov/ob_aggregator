import grpc
import click
import keyrock_ob_aggregator_pb2
import keyrock_ob_aggregator_pb2_grpc
from google.protobuf.json_format import MessageToDict
from rich.table import Table
from rich.live import Live
from rich.layout import Layout
from rich.pretty import Pretty
from rich.panel import Panel

layout = Layout()


def generate_table(data):
    if data and 'spread' in data.keys():
        spread = str(round(data['spread'], 5))
        data['spread'] = float(spread)
    else:
        spread = ""
    layout.split_row(
        Layout(name="left"),
        Layout(name="right")
    )
    table = Table(expand=True)
    table.add_column("SIDE")
    table.add_column("PRICE")
    table.add_column("SIZE")
    table.add_column("EXCHANGE")

    if data:
        for ask in data['asks'][::-1]:  # red
            table.add_row("ASK", str(ask['price']), str(ask['amount']), ask['exchange'], style='red')
        table.add_row()

        if 'spread' in data.keys():
            table.add_row("SPREAD", spread, style='blue')

        table.add_row()
        for bid in data['bids']:  # green
            table.add_row("BID", str(bid['price']), str(bid['amount']), bid['exchange'], style='green')

    layout["left"].update(
        Panel(table, title="Aggregated Orderbook")
    )
    layout["right"].update(
        Panel(Pretty(data), title="Raw Data")
    )
    return layout


@click.command()
@click.option("--port", type=int, default=50052)
def run_client(port):
    """
    Simple client that listens to messages from the server
    and updates a TUI table in live mode.
    """
    try:
        channel = grpc.insecure_channel(f'localhost:{port}')
        stub = keyrock_ob_aggregator_pb2_grpc.OrderbookAggregatorStub(channel)
        empty = keyrock_ob_aggregator_pb2.Empty()

        with Live(generate_table([]), refresh_per_second=5) as live:
            for data in stub.BookSummary(empty):
                live.update(generate_table(MessageToDict(data)))
    except KeyboardInterrupt:
        pass
    except grpc._channel._MultiThreadedRendezvous as e:
        print(f"RPC Server Error: \n{e}")


if __name__ == "__main__":
    run_client()

