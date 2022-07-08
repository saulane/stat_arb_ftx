from yaml import AnchorToken
from ftxclient import FTXClient
from ftxwebsocketclient import FTXWebsocketClient
from pairs import Future, Pairs, get_all_futures_filtered
import time
from statsmodels.tsa.stattools import coint
from statsmodels.tsa.vector_ar.vecm import coint_johansen


if __name__ == "__main__":
    client = FTXClient()
    ws_client = FTXWebsocketClient()
    ws_client.connect()
    futures = get_all_futures_filtered(client)
    # print(client.get_historical_prices("BTC-PERP"))
    futures_prices = []
    for future in futures[:2]:
        futures_prices.append(Future(future, client.get_historical_prices(future, resolution=300,start_time=time.time()-300000, end_time=time.time())))
    

    ws_client.subscribe({"channel": "ticker", "market": "ETH-PERP"})

    print(futures_prices[0].resolution)
    ws_client.unsubscribe({"channel": "ticker", "market": "ETH-PERP"})

    # for i in range(len(futures_prices)):
    #     for j in range(i+1, len(futures_prices)):
    #         cointe = coint(futures_prices[i].closes, futures_prices[j].closes)
    #         if cointe[0] < cointe[2][0]:
    #             print(futures_prices[i].id, "/",futures_prices[j].id)