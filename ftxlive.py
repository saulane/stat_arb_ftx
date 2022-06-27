from tokenize import String
from websocket import WebSocketApp
import yaml
import json

from collections import defaultdict, deque
from typing import DefaultDict, Deque, List, Dict, Tuple, Optional

from threading import Thread, Lock

import hashlib
import hmac
import time

with open("config.yaml", "r") as ymlfile:
    cfg = yaml.safe_load(ymlfile)
    print("Config file loaded")

class FTXWebsocketManager():
    _ENDPOINT = 'wss://ftx.com/ws/'
    _CONNECT_TIMEOUT_S = 100

    def __init__(self, api_key, api_secret) -> None:
        self.connect_lock = Lock()
        self.api_key = api_key
        self.api_secret = api_secret
        self._subscriptions: List[Dict] = []
        self._tickers: DefaultDict[str, Dict] = defaultdict(dict)
        self.ws = None

    def _get_url(self) -> str:
        return self._ENDPOINT

    def _connect(self):
        assert not self.ws, "ws should be closed before attempting to connect"

        self.ws = WebSocketApp(
            self._get_url(),
            on_message=self._wrap_callback(self._on_message),
            on_close=self._wrap_callback(self._on_close),
            on_error=self._wrap_callback(self._on_error),
        )

        wst = Thread(target=self._run_websocket, args=(self.ws,))
        wst.daemon = True
        wst.start()

        # Wait for socket to connect
        ts = time.time()
        while self.ws and (not self.ws.sock or not self.ws.sock.connected):
            if time.time() - ts > self._CONNECT_TIMEOUT_S:
                self.ws = None
                return
            time.sleep(0.1)

    def connect(self):
        if self.ws:
            return
        with self.connect_lock:
            while not self.ws:
                self._connect()

    def send(self, message):
        self.connect()
        self.ws.send(message)

    def send_json(self, message):
        self.send(json.dumps(message))

    def _on_open(self,ws,message):
        print("Opened")

    def _login(self) -> None:
        ts = int(time.time() * 1000)
        self.send_json({'op': 'login', 'args': {
            'key': self._api_key,
            'sign': hmac.new(
                self._api_secret.encode(), f'{ts}websocket_login'.encode(), 'sha256').hexdigest(),
            'time': ts,
        }})
        self._logged_in = True
    
    def _on_message(self, ws, raw_message: str):
        print("message")
        message = json.loads(raw_message)
        message_type = message['type']
        if message_type in {'subscribed', 'unsubscribed'}:
            return
        elif message_type == 'info':
            if message['code'] == 20001:
                return self.reconnect()
        elif message_type == 'error':
            raise Exception(message)

        channel = message["channel"]
        
        if channel == 'ticker':
            self._handle_ticker_message(message)

    def _on_error(self, ws, error):
        return error

    def get_ticker(self, market: str) -> Dict:
        subscription = {'channel': 'ticker', 'market': market}
        if subscription not in self._subscriptions:
            self._subscribe(subscription)
        return self._tickers[market]

    def _wrap_callback(self, f):
        def wrapped_f(ws, *args, **kwargs):
            if ws is self.ws:
                try:
                    f(ws, *args, **kwargs)
                except Exception as e:
                    raise Exception(f'Error running websocket callback: {e}')
        return wrapped_f

    def _run_websocket(self, ws):
        print("running")
        try:
            ws.run_forever()
        except Exception as e:
            raise Exception(f'Unexpected error while running websocket: {e}')
        finally:
            self._reconnect(ws)

    def _reconnect(self, ws):
        assert ws is not None, '_reconnect should only be called with an existing ws'
        if ws is self.ws:
            self.ws = None
            ws.close()
            self.connect()

    def reconnect(self) -> None:
        if self.ws is not None:
            self._reconnect(self.ws)

    def _on_close(self, ws):
        self._reconnect(ws)

    def _subscribe(self, subscription: Dict) -> None:
        self.send_json({'op': 'subscribe', **subscription})
        self._subscriptions.append(subscription)

    def _unsubscribe(self, subscription: Dict) -> None:
        self.send_json({'op': 'unsubscribe', **subscription})
        while subscription in self._subscriptions:
            self._subscriptions.remove(subscription)

    def _handle_ticker_message(self, message: Dict) -> None:
        self._tickers[message['market']] = message["data"]



ftx = FTXWebsocketManager(cfg["ftx_api_key"], cfg["ftx_api_secret"])
print(ftx.get_ticker("BEAR/USD"))














# def on_open(ws):
#     ts = int(time.time() * 1000)
#     ws.send(json.dumps({'op': 'login', 'args': {
#             'key': cfg['ftx_api_key'],
#             'sign': hmac.new(
#                 cfg['ftx_api_secret'].encode(), f'{ts}websocket_login'.encode(), 'sha256').hexdigest(),
#             'time': ts,
#         }}))

# def on_message(ws, raw_msg):
#     msg = json.loads(raw_msg)
#     print(msg)

# def on_error(ws, error):
#     print(error)

# ws = websocket.WebSocketApp("wss://ftx.com/ws/", on_open=on_open, on_message=on_message, on_error=on_error)
# ws.run_forever()
