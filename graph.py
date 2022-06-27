from typing import Dict, List
from ftxwebsocketclient import FTXWebsocketClient
import networkx as nx

class CoinsGraph():

    def __init__(self, tickers: List[Dict]) -> None:
        markets = [tick["market"] for tick in tickers]
        self.nodes = list(map(lambda x: x.split("/"), markets))


t = [{"market": "BTC/ETH"},{"market": "BTC/PO"},{"market": "KU/ETH"},{"market": "BNB/ETH"}]

tc = CoinsGraph(t)
print(tc.nodes)