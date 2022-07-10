from yaml import AnchorToken
from ftx import FTXClient, FTXWebsocketClient
from pairs import Future, Pairs, get_all_futures_filtered
import time
from database import Database

from tqdm import tqdm


if __name__ == "__main__":
    client = FTXClient()
    # ws_client = FTXWebsocketClient()
    # ws_client.connect()
    # futures_id = get_all_futures_filtered(client)
    # print(client.get_historical_prices("BTC-PERP"))
    # futures = []
    # for future in tqdm(futures_id, desc="Gathering Futures prices"):
    #     futures.append(Future(future, client.get_historical_prices(future, resolution=300,start_time=time.time()-300000, end_time=time.time())))
    

    # coints = []
    # for i in range(len(futures)):
    #     for j in range(i+1, len(futures)):
    #         pair = Pairs(futures[i], futures[j])

    #         if pair.is_coint():
    #             coints.append(pair)
    #             print(f"{pair.id} are cointegrated, {pair.is_opportunity()}")
    #         else:
    #             del pair

    db = Database("db/arbitrage.db")
    ws_client = FTXWebsocketClient(db)
    ws_client.connect()

    ws_client.subscribe({"channel": "ticker", "market": "ETH-PERP"})
    ws_client.subscribe({"channel": "ticker", "market": "SOL-PERP"})