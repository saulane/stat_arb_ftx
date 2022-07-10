import sqlite3
import time

class Database():
    def __init__(self, file) -> None:
        self.conn = None
        self._file = file

        self._create_live_table()
        self._create_pairs_table()

    def _create_connection(self):
        """ create a database connection to a SQLite database"""
        try:
            conn = sqlite3.connect(self._file)
        except sqlite3.Error as e:
            print(e)
        
        return conn

    def _create_live_table(self):
        live_table = """CREATE TABLE IF NOT EXISTS live_prices (
                                name text PRIMARY KEY NOT NULL,
                                price real,
                                last_update integer
                                );"""
        try:
            conn = self._create_connection()
            cur = conn.cursor()
            cur.execute(live_table)
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
            conn = self._create_connection()
            cur = conn.cursor()
            # cur.execute("""DROP TABLE IF EXISTS pairs;""")
            cur.execute(pairs_table)
            cur.execute(price_trigger)
            cur.execute(pairs_trigger)
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
        conn = self._create_connection()
        cur = conn.cursor()
        cur.execute(sql, pair)
        conn.commit()

    def update_pair(self, id, a_price, b_price):
        pair = (a_price, b_price, id)
        sql = """UPDATE pairs
                SET a_price = ?, b_price = ?
                WHERE id = ?"""

        conn = self._create_connection()
        curr = conn.cursor()
        curr.execute(sql, pair)
        conn.commit()

    def delete_pair(self, id):
        sql = """DELETE FROM pairs WHERE id=?;"""

        conn = self._create_connection()
        curr = conn.cursor()
        curr.execute(sql, (id,))
        conn.commit()

    def update_crypto(self, name, price):
        crypto = (name, price, int(time.time()), price, int(time.time()))
        sql = """INSERT INTO live_prices(name, price, last_update) 
                VALUES(?,?,?)
                ON CONFLICT(name) DO UPDATE 
                SET price=?, last_update=?"""

        conn = self._create_connection()
        cur = conn.cursor()
        cur.execute(sql, crypto)
        conn.commit()

    def get_all_crypto_prices(self):
        conn = self._create_connection()
        cur = conn.cursor()
        cur.execute("SELECT * FROM live_prices")

        rows = cur.fetchall()

        return rows


db = Database(r"db/arbitrage.db")
# db.update_crypto("ETH", 12789)
# db.update_crypto("BCH", 127)
# print(db.get_all_crypto_prices())

# db.delete_pair("BTC/ETH")
# db.add_pair("BTC", "ETH", 97.0, 12)
# db.update_pair("BTC/ETH", 90, 10)

# conn = create_connection(r"db/arbitrage.db")

# update_crypto(conn, "BTC", 28.8)