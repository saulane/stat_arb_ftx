from re import sub
import time
import json
import websocket
from threading import Thread, Lock

from typing import DefaultDict, Deque, List, Dict, Tuple, Optional, final

# websocket.enableTrace(True)

class FTXWebsocketClient():
    _ENDPOINT = "wss://ftx.com/ws/"

    def __init__(self):
        self.ws: websocket.WebSocketApp = None
        self.ws_thread: Thread = None
        self.ws_lock: Lock = Lock()

        self.subscriptions: List[Dict] = []
        self.tickers: Dict[Dict] = {}
        self.trades: Dict[Dict] = {}

        self.stored_tickers = {}
    
    def _get_url(self):
        return self._ENDPOINT


    def _process_ticker(self,ticker):
        tick_name = ticker["market"]
        data = ticker["data"]
        last = data["last"]


        self.stored_tickers[tick_name] = last
        # self.stored_tickers[tick_name].append(last)

    def _on_message(self, ws, raw_msg):
        msg = json.loads(raw_msg)
        msg_type = msg["type"]

        if msg_type != "update":
            return

        if msg["channel"] == "ticker":
            self.tickers[msg["market"]] = msg["data"]
            # print(msg)
            self._process_ticker(msg)
            print(self.stored_tickers)
        elif msg["channel"] == "trade":
            self.trades[msg["market"]] = msg["data"]



    def _on_error(self, ws, error):
        print("Problème:", error, ws)

    def _run_websocket(self):
        try:
            self.ws.run_forever()
        except Exception as e:
            raise Exception(f'Unexpected error while running websocket: {e}')

    
    def _connect(self):
        self.ws = websocket.WebSocketApp(
            "wss://ftx.com/ws/",
            on_message=self._on_message,
            on_error=self._on_error,
        )

        wst = Thread(target=self.ws.run_forever)
        wst.start()

        # Wait for socket to connect
        ts = time.time()
        while self.ws and (not self.ws.sock or not self.ws.sock.connected):
            if time.time() - ts > 10:
                self.ws = None
                return
            time.sleep(0.1)

    def connect(self):
        if self.ws:
            return
        with self.ws_lock:
            while not self.ws:
                self._connect()

    def send(self,msg):
        self.ws.send(msg)

    def send_json(self, msg):
        self.send(json.dumps(msg))

    def subscribe(self, subscription):
        if subscription in self.subscriptions:
            return
        self.subscriptions.append(subscription)
        
        self.ws_lock.acquire()
        try:
            self.send_json({"op": "subscribe", **subscription})
        finally:
            self.ws_lock.release()

    def unsubscribe(self, subscription):
        if not subscription in self.subscriptions:
            return
        
        while subscription in self.subscriptions:
            self.subscriptions.remove(subscription)
        self.ws_lock.acquire()
        try:
            self.send_json({"op": "unsubscribe", **subscription})
        finally:
            self.ws_lock.release()


test = FTXWebsocketClient()
test.connect()
test.subscribe({"channel": "ticker", "market": "ETH-PERP"})
test.subscribe({"channel": "ticker", "market": "ADA-PERP"})