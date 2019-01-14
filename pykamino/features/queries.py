import pandas

from pykamino.db import Trade, database
from pykamino.features.orders import OrdersDataFrame

_orders_query = """
    SELECT side, price, amount, insert_ts, remove_ts
      FROM data.orders
     WHERE product = 'BTC-USD' AND
           insert_ts <= %(end)s AND
           (remove_ts > %(start)s OR remove_ts IS NULL)
"""


def orders_in_time_window(start_ts, end_ts, product):
    start = start_ts.strftime("%Y-%m-%d %H:%M:%S.%f")
    end = end_ts.strftime("%Y-%m-%d %H:%M:%S.%f")
    data = pandas.read_sql_query(
        _orders_query,
        con=db_conn,
        params={"start": start, "end": end},
        parse_dates={
            "insert_ts": "%Y-%m-%d %H:%M:%S.%f",
            "remove_ts": "%Y-%m-%d %H:%M:%S.%f"
        }
    )
    return OrdersDataFrame(data)

