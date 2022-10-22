import threading
import websocket


class WSClient(threading.Thread):
    def __init__(self, endpoint, exchange_name, logger):
        super().__init__()

        self._ws = websocket.WebSocketApp(
            url=endpoint,
            on_message=self.__on_message,
            on_error=self.__on_error,
            on_close=self.__on_close,
            on_open=self.__on_open
        )

        self._exchange_name = exchange_name
        self._logger = logger

    def run(self):
        while True:
            self._ws.run_forever()

    def __on_message(self, message):
        raise NotImplementedError

    def __on_error(self, error):
        self._logger.error(f"Error with {self._exchange_name}: {error}")

    def __on_close(self):
        self._logger.info(f"Closed connection to {self._exchange_name}")

    def __on_open(self):
        self._logger.info(f"Connected to {self._exchange_name}")

