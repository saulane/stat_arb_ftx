import numpy as np
import pandas as pd
from statsmodels.tsa.stattools import adfuller, coint
from ftxclient import FTXClient
from typing import Any, Dict, List
import pandas as pd
from collections import deque

class Future():
    def __init__(self, id: str, hist: List[Dict]) -> None:
        self.id = id.split("-", maxsplit=1)[0]

        self._raw_hist = hist

        self.historical_data = pd.DataFrame(hist)
        
        self.resolution = (self.historical_data["time"][1] - self.historical_data["time"][0]) / 1000
        self.start = self.historical_data["startTime"][0]
        self.end = self.historical_data["startTime"].values[-1]

class Pairs():
    def __init__(self, a: Future = None, b: Future = None) -> None:
        if len(a.historical_data)!= len(b.historical_data):
            raise ValueError("Historical data must be the same size")
        self.id = a.id + "/" +b.id
        self.a = a
        self.b = b
        self.cointegration_test_result = None

        self.spread_hist = abs(self.a.historical_data["close"] - self.b.historical_data["close"])
        self.spread_mean = np.mean(self.spread_hist)
        self.spread_std = np.std(self.spread_hist)

        self.curr_spread = self.spread_hist.values[-1]
    
    def update_spread(self, a = None, b = None):
        self.curr_spread = abs(a - b)
        return self.is_opportunity()

    def test_cointegration(self):
        res = coint(self.a.historical_data["close"].values, self.b.historical_data["close"].values)
        return res

    def is_opportunity(self) -> bool:
        return self.curr_spread > self.spread_mean+2*self.spread_std

    def is_coint(self) -> bool:
        self.cointegration_test_result = self.test_cointegration()
        return self.cointegration_test_result[0] < self.cointegration_test_result[2][0]

    def __repr__(self) -> str:
        return f"{self.id!r} spread std is {self.spread_std!r}"

def get_all_futures_filtered(client: FTXClient):
    futures = client.get_all_futures()
    filtered = []
    for i in futures:
        if i["perpetual"] == True and i["volumeUsd24h"] >= 10000000:
            filtered.append(i["name"])

    return filtered

# def make_pairs(futures):
#     n = len(futures)
#     pairs = []
#     for i in range(n):
#         for j in range(i+1, n):
#             pairs.append(Pairs(f"{futures[i]}/{futures[j]}"))

#     return list(map(lambda x: x.id, pairs))

# print(make_pairs(get_all_futures_filtered(client)))

