import pandas as pd
import numpy as np
import time

class Trade():
    def __init__(self, symbol, entry, size, long = True) -> None:
        self.symbol = symbol
        self.entry = entry
        self.size = size
        self.long = long
        self.exit = None

        self.entry_time = time.time()

        self.is_close = False

        self.pct_profit = None
        self.profit = None
        pass

    def close(self, exit_price):
        self.exit = exit_price
        self.exit_time = time.time()
        self.is_close = True
        
        self.pct_profit = (self.exit - self.entry)/self.entry if self.long else (self.entry - self.exit)/self.entry
        self.profit = (self.exit - self.entry)*self.size if self.long else (self.entry - self.exit)*self.size

    def __repr__(self) -> str:
        side = "bought" if self.long else "sold"
        trading = "still open" if not self.is_close else f"exited at {self.exit}"
        return f"{self.size} {self.symbol!r} {side!r} at {self.entry!r} {trading!r}"

    def to_tuple(self):
        return (self.symbol, self.entry, self.long, self.entry_time)

class PairTrade():
    def __init__(self, a, b, a_price, b_price, size, a_long = True) -> None:
        a_size = (size/2) / a_price
        b_size = (size/2) / b_price
        self.a_trade = Trade(a, a_price, a_size, long=a_long)
        self.b_trade = Trade(b, b_price, b_size, long=(not a_long))

        self.profit = None
        self.pct_profit = None

        self.is_close = False
        
    def close(self, a_exit, b_exit):
        self.a_trade.close(a_exit)
        self.b_trade.close(b_exit)

        self.profit = self.a_trade.profit + self.b_trade.profit
        self.pct_profit = np.mean([self.a_trade.pct_profit, self.b_trade.pct_profit])

        self.is_close = True
