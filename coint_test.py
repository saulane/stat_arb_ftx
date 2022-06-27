from statsmodels.tsa.stattools import coint
from statsmodels.tsa.vector_ar.vecm import coint_johansen


class CointegrationTester():
    def __init__(self, tickers) -> None:
        
        self.tickers = tickers

        self.coint_pairs = None

    def engle_granger(self):
        for i in self.tickers:
            for j in self.tickers:
                if i != j:
                    coint_res = coint(i, j)
    