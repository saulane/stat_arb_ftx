from typing import List
from yaml import AnchorToken
from ftx import FTXClient, FTXWebsocketClient
from pairs import Future, Pairs, get_all_futures_filtered
import time
from database import Database
import pandas as pd
import numpy as np

from tqdm import tqdm


if __name__ == "__main__":
    client = FTXClient()
    futures_id = get_all_futures_filtered(client)
    # print(client.get_historical_prices("BTC-PERP"))
    futures = []
    for future in tqdm(futures_id[:20], desc="Gathering Futures prices"):
        futures.append(Future(future, client.get_historical_prices(future, resolution=300,start_time=time.time()-300000, end_time=time.time())))
    

    db = Database("db/arbitrage.db")
    list_crypto = []
    coints: List[Pairs] = []
    for i in range(len(futures)):
        for j in range(i+1, len(futures)):
            pair = Pairs(futures[i], futures[j])

            if pair.is_coint():
                coints.append(pair)
                db.add_pair(pair.a_id, pair.b_id, 0, 0)
                list_crypto.extend([pair.a_id, pair.b_id])

                print(f"{pair.id} are cointegrated, {pair.check_opportunity()}")
            else:
                del pair
    set_crypto = set(list_crypto)
    ws_client = FTXWebsocketClient(db)
    ws_client.connect()
    for i in set_crypto:
        ws_client.subscribe({"channel": "ticker", "market": f"{i}-PERP"})
        print("Subscribed to", i)

    while True:
        current_prices = pd.DataFrame(db.get_all_crypto_prices(), columns=["Name", "Price", "Last_update"])
        gathered_list = current_prices["Name"].values
        current_prices.set_index("Name", inplace=True)

        for i in coints:
            print("Updating", i.id)

            a = i.a_id
            b = i.b_id
            if a in gathered_list and b in gathered_list:
                a_live = current_prices.loc[a]["Price"]
                b_live = current_prices.loc[b]["Price"]
                i.update_spread(a_live, b_live)
                print(f"New spread for {i.id} is {i.curr_spread}")
        time.sleep(.5)
