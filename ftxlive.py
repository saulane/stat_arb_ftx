import websocket

def on_open(ws):
    ws.send('{"op":"subscribe","channel":"trades","market":"BTC-PERP"}')


def on_message(ws, message):
    print(message)

def on_error(ws, error):
    print(error)

ws = websocket.WebSocketApp("wss://ftx.com/ws/", on_open=on_open, on_message=on_message, on_error=on_error)
ws.run_forever()
