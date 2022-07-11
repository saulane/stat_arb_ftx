import sqlite3
import time

class Database():
    def __init__(self, file) -> None:
        self.__file = file

        try:
            self.__connection = sqlite3.connect(self.__file)
            self.__cursor = self.__connection.cursor()
        except sqlite3.Error as e:
            print(e)

        self._create_live_table()
        self._create_pairs_table()

    def __enter__(self):
        return self

    def __exit__(self, ext_type, exc_value, traceback):
        self.__cursor.close()
        if isinstance(exc_value, Exception):
            self.__connection.rollback()
        else:
            self.__connection.commit()
        self.__connection.close()

    def __del__(self):
        self.__connection.close()

    def _create_live_table(self):
        live_table = """CREATE TABLE IF NOT EXISTS live_prices (
                                name text PRIMARY KEY NOT NULL,
                                price real,
                                last_update integer
                                );"""
        try:
            self.__cursor.execute(live_table)
        except sqlite3.Error as e:
            print(e)

    def _create_pairs_table(self):
        pairs_table = """CREATE TABLE IF NOT EXISTS pairs(
                            id text PRIMARY KEY,
                            a_name text NOT NULL,
                            b_name text NOT NULL,
                            a_price real,
                            b_price real,
                            spread real
                            );"""

        pairs_trigger = """CREATE TRIGGER IF NOT EXISTS compute_spread_after_update AFTER UPDATE ON pairs 
                            BEGIN
                                UPDATE pairs SET spread = ABS(new.a_price - new.b_price)
                                WHERE id = new.id;
                            END;"""

        price_trigger = """CREATE TRIGGER IF NOT EXISTS update_prices AFTER UPDATE ON live_prices
                            BEGIN
                                UPDATE pairs SET a_price = new.price
                                WHERE a_name=new.name;
                                UPDATE pairs SET b_price = new.price
                                WHERE b_name=new.name;
                            END;"""
        try:

            # cur.execute("""DROP TABLE IF EXISTS pairs;""")
            self.__cursor.execute(pairs_table)
            self.__cursor.execute(price_trigger)
            self.__cursor.execute(pairs_trigger)
        except sqlite3.Error as e:
            print(e)

    def add_pair(self, a_name, b_name, a_price, b_price):
        id = a_name + "/" + b_name
        spread = abs(a_price - b_price)
        pair =  (id, a_name, b_name, a_price, b_price, spread, a_price, b_price, spread)
        sql = """INSERT INTO pairs(id, a_name, b_name, a_price, b_price, spread)
                    VALUES(?,?,?,?,?,?)
                    ON CONFLICT(id) DO UPDATE
                    SET a_price=?, b_price=?, spread=?"""   

        self.__cursor.execute(sql, pair)

    def update_pair(self, id, a_price, b_price):
        pair = (a_price, b_price, id)
        sql = """UPDATE pairs
                SET a_price = ?, b_price = ?
                WHERE id = ?"""


        self.__cursor.execute(sql, pair)

    def delete_pair(self, id):
        sql = """DELETE FROM pairs WHERE id=?;"""

        self.__cursor.execute(sql, (id,))

    def update_crypto(self, name, price):
        crypto = (name, price, int(time.time()), price, int(time.time()))
        sql = """INSERT INTO live_prices(name, price, last_update) 
                VALUES(?,?,?)
                ON CONFLICT(name) DO UPDATE 
                SET price=?, last_update=?"""

        self.__cursor.execute(sql, crypto)

    def get_all_crypto_prices(self):
        self.__cursor.execute("SELECT * FROM live_prices")

        rows = self.__cursor.fetchall()

        return rows

    def get_all_spread(self):
        self.__cursor.execute("SELECT * from pairs")

        rows = self.__cursor.fetchall()

        return rows


db = Database(r"db/arbitrage.db")
with db:
    db.update_crypto("ETH", 12789)
# db.update_crypto("BCH", 127)
# print(db.get_all_crypto_prices())

# db.delete_pair("BTC/ETH")
# db.add_pair("BTC", "ETH", 97.0, 12)
# db.update_pair("BTC/ETH", 90, 10)

# conn = create_connection(r"db/arbitrage.db")

# update_crypto(conn, "BTC", 28.8)