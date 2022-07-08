import numpy as np
import pandas as pd
from statsmodels.tsa.stattools import adfuller, coint
from ftxclient import FTXClient
from typing import Any, Dict, List
import pandas as pd
from collections import deque

class Future():
    def __init__(self, id: str, hist: List[Dict]) -> None:
        self.id = id

        self.raw_hist = hist

        self.historical_data = pd.DataFrame(hist)

        self.current_prices = deque([], maxlen = 100000)

        self.closes: List = [i["close"] for i in hist]
        self.highs: List = [i["high"] for i in hist]
        self.lows: List = [i["low"] for i in hist]
        self.opens: List = [i["open"] for i in hist]
        
        self.resolution = (self.historical_data["time"][1] - self.historical_data["time"][0]) / 1000
        self.start = self.historical_data["startTime"][0]
        self.end = self.historical_data["startTime"].values[-1]

class Pairs():
    def __init__(self, id, a = None, b = None) -> None:
        self.id = id
        self.a = a
        self.b = b
        self.coint_res = None
        pass
    
    def update(self, a = None, b = None):
        self.a = a if a != None else self.a
        self.b = b if b != None else self.b

    def push(self, a = None, b = None):
        if a != None:
            a.append(a)
            dropped = a.pop(0)
        
        if b != None:
            b.append(b)
            dropped = b.pop(0)

    def coint_test(self):
        res = coint(self.a, self.b)
        self.coint_res = res
        return res

    def is_coint(self) -> bool:
        test_result = self.coint_test()
        if test_result[0] < test_result[2][0]:
            return True
        else:
            return False

client = FTXClient()

def get_all_futures_filtered(client: FTXClient):
    futures = client.get_all_futures()
    filtered = []
    for i in futures:
        if i["perpetual"] == True and i["volumeUsd24h"] >= 10000000:
            filtered.append(i["name"])

    return filtered

def make_pairs(futures):
    n = len(futures)
    pairs = []
    for i in range(n):
        for j in range(i+1, n):
            pairs.append(Pairs(f"{futures[i]}/{futures[j]}"))

    return list(map(lambda x: x.id, pairs))

# print(make_pairs(get_all_futures_filtered(client)))

