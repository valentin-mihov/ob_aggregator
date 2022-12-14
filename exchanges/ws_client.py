import threading
import websocket
import logging


class WSClient(threading.Thread):
    def __init__(self, endpoint: str, exchange_name: str, logger: logging.Logger):
        """
        Threaded WebSocket Client
        :param endpoint: WS endpoint
        :param exchange_name: exchange name
        :param logger: logging object
        """
        super().__init__()

        self._ws = None
        self._endpoint = endpoint
        self._exchange_name = exchange_name
        self._ws = websocket.WebSocketApp(
            url=self._endpoint,
            on_message=self._on_message,
            on_error=self._on_error,
            on_close=self._on_close,
            on_open=self._on_open
        )
        self._logger = logger

    def run(self):
        while True:
            self._ws.run_forever()

    def _on_message(self, wsapi, message):
        raise NotImplementedError

    def _on_error(self, wsapi, error):
        self._logger.error(f"Error with {self._exchange_name}: {error}")

    def _on_close(self, wsapi, close_status_code, close_msg):
        self._logger.info(f"Closed connection to {self._exchange_name}")

    def _on_open(self, wsapi):
        self._logger.info(f"Connected to {self._exchange_name}")

