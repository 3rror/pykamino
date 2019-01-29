import multiprocessing
from datetime import datetime
from functools import lru_cache as cache
from statistics import mean

import numpy as np
import pandas
from pykamino.db import Order, OrderHistory, database

# Subclassing pandas.DataFrame
# http://pandas.pydata.org/pandas-docs/stable/extending.html#extending-subclassing-pandas


class OrdersSeries(pandas.Series):
    @property
    def _constructor(self):
        return OrdersSeries

    @property
    def _constructor_expanddim(self):
        return OrdersDataFrame


class OrdersDataFrame(pandas.DataFrame):
    @property
    def _constructor(self):
        return OrdersDataFrame

    @property
    def _constructor_sliced(self):
        return OrdersSeries

    def at_timestamp(self, timestamp):
        """Extract a subset of orders that are open in specified timestamp."""
        filter = (self.time <= timestamp) & (
            (self.close_time > timestamp) | self.close_time.isnull()
        )
        return self[filter]


class OrderBook:
    """
    Represents an order book.
    An order book is an electronic list of buy and sell orders for a specific
    security or financial instrument.
    """

    def __init__(self, orders, timestamp):
        self.orders = orders
        self.timestamp = timestamp

    @cache(maxsize=1)
    def asks(self):
        """Ask orders ordered by price."""
        asks_only_mask = self.orders.side == "ask"
        return self.orders[asks_only_mask]

    @cache(maxsize=1)
    def bids(self):
        """Bid orders ordered by price."""
        bids_only_mask = self.orders.side == "bids"
        return self.orders[bids_only_mask]

    @cache(maxsize=1)
    def best_ask_order(self):
        """
        Ask order with the minimimum price.
        If there are more orders with the same price, the one with the
        maximum amount is returned.
        """
        return (
            self.asks()
            .sort_values(["price", "size"], ascending=[True, False])
            .iloc[0]
        )

    @cache(maxsize=1)
    def best_bid_order(self):
        """
        Bid order with the maximum price.
        If there are more orders with the same price, the one with the
        minimum amount is returned.
        """
        return (
            self.bids()
            .sort_values(["price", "size"], ascending=[True, False])
            .iloc[-1]
        )

    @cache(maxsize=1)
    def best_ask_price(self):
        """Minimum price among ask orders."""
        return self.asks().price.astype(float).min()

    @cache(maxsize=1)
    def best_bid_price(self):
        """Maximum price among bid orders."""
        return self.bids().price.astype(float).max()

    @cache(maxsize=1)
    def best_ask_amount(self):
        """
        Total amount of assets of the ask orders at the best price.
        The best ask price is the minimum price sellers are willing to
        accept.
        """
        best_price_mask = self.asks().price == self.best_ask_price()
        return (self.asks()[best_price_mask].sum().amount)

    @cache(maxsize=1)
    def best_bid_amount(self):
        """
        Total amount of assets of the bid orders at the best price.
        The best bid price is the maximum price buyers are willing to
        pay.
        """
        best_price_mask = self.bids().price == self.best_bid_price()
        return (self.bids()[best_price_mask].sum().amount)

    @cache(maxsize=1)
    def mid_market_price(self):
        """
        Mean between the best bid price and the best ask price.
        The mid market price represents an accurate estimate of the true price
        of the asset (BTC in this case) at one instant.
        """
        m = mean([self.best_bid_price(), self.best_ask_price()])
        return round(m, 8)

    def spread(self):
        """
        Difference between the highest price that a buyer is willing to pay
        (bid) and the lowest price that a seller is willing to accept (ask).
        Small spreads generate a frictionless market, where trades can occur
        with no significant movement of the price.
        """
        spread = self.best_bid_price() - self.best_ask_price()
        return round(spread, 8)

    def ask_depth(self):
        """Number of ask orders."""
        return len(self.asks())

    def bid_depth(self):
        """Number of bid orders."""
        return len(self.bids())

    @cache(maxsize=1)
    def ask_depth_chart(self):
        return  (
            self.asks()
            .sort_values(by="price")
            .groupby("price")
            .sum().amount
            .cumsum().reset_index()
        )

    @cache(maxsize=1)
    def bid_depth_chart(self):
        return (
            self.bids()
            .sort_values(by="price")
            .groupby("price")
            .sum().amount
            .iloc[::-1]
            .cumsum()
            .iloc[::-1]
            .reset_index()
        )

    @cache(maxsize=1)
    def ask_depth_chart_bins(self, bins=10):
        ask_part = self.ask_depth_chart()
        ask_part = ask_part[ask_part.price < 1.99 * self.mid_market_price()]
        ask_bins = ask_part.groupby(
            pandas.cut(
                ask_part.price,
                np.linspace(ask_part.price.min(), ask_part.price.max(), bins),
            )
        )
        ask_samples = ask_bins.mean().amount.tolist()
        return ask_samples

    @cache(maxsize=1)
    def bid_depth_chart_bins(self, bins=10):
        bid_part = self.bid_depth_chart()
        bid_part = bid_part[bid_part.price > 0.01 * self.mid_market_price()]
        bid_bins = bid_part.groupby(
            pandas.cut(
                bid_part.price,
                np.linspace(bid_part.price.min(), bid_part.price.max(), bins),
            )
        )
        bid_samples = bid_bins.mean().iloc[::-1].amount.tolist()
        return bid_samples

    @cache(maxsize=2)
    def ask_volume_weighted(self, price_weight=1):
        return self._volume_weighted_by_price(self.asks(), price_weight)

    @cache(maxsize=2)
    def bid_volume_weighted(self, price_weight=1):
        return self._volume_weighted_by_price(self.bids(), price_weight)

    def features(self):
        """Dictionary of all the features in this order book"""
        return {
            "mid_market_price": self.mid_market_price(),
            "best_ask_price": self.best_ask_price(),
            "best_bid_price": self.best_bid_price(),
            "best_ask_amount": self.best_ask_amount(),
            "best_bid_amount": self.best_bid_amount(),
            "market_spread": self.spread(),
            "ask_depth": self.ask_depth(),
            "bid_depth": self.bid_depth(),
            "ask_volume": self.ask_volume_weighted(price_weight=0),
            "bid_volume": self.bid_volume_weighted(price_weight=0),
            "ask_volume_weighted": self.ask_volume_weighted(),
            "bid_volume_weighted": self.bid_volume_weighted(),
            **self._ask_depth_chart_bins(),
            **self._bid_depth_chart_bins(),
            "timestamp": self.timestamp,
        }

    def _ask_depth_chart_bins(self):
        return {
            f"ask_depth_chart_bin{i}": self.ask_depth_chart_bins(11)[i]
            for i in range(10)
        }

    def _bid_depth_chart_bins(self):
        return {
            f"bid_depth_chart_bin{i}": self.bid_depth_chart_bins(11)[i]
            for i in range(10)
        }

    def _volume_weighted_by_price(self, orders, price_weight=1):
        mid_price = self.mid_market_price()
        return orders.amount.dot(
            orders.price.subtract(mid_price).apply(
                lambda x: abs(1 / -x) ** price_weight
            )
        )


def _order_books_features(orders, timestamp):
    orders_at_ts = orders.at_timestamp(timestamp)
    order_book = OrderBook(orders_at_ts, timestamp)
    return order_book.features()


_query = """
WITH oh_with_max AS
(
  SELECT
    oh1.amount,
    oh1.time,
    oh1.order_id
  FROM
    exchange.order_history oh1
    JOIN
      (
        SELECT
          order_id,
          max(time) AS time
        FROM
          exchange.order_history
        GROUP BY
          order_id
      ) oh2
      ON oh1.order_id = oh2.order_id
      AND oh1.time = oh2.time
  WHERE
    oh1.time <= %s
)
---
SELECT
  o.side,
  o.price,
  o.close_time,
  oh_with_max.time,
  oh_with_max.amount
FROM
  exchange.order o
  JOIN
    oh_with_max
    ON o.id = oh_with_max.order_id
WHERE
  (o.close_time IS NULL
  OR o.close_time > %s)
  AND o.product IN %s
"""


def orders_in_time_window(start_ts, end_ts, products):
    start = datetime.strftime(start_ts, Order.close_time.formats[0])
    end = datetime.strftime(end_ts, OrderHistory.time.formats[0])
    data = pandas.read_sql_query(
        _query,
        con=database.connection(),
        params=(end, start, tuple(products)),
    )
    return OrdersDataFrame(data)


def extract(start_dt, end_dt, resolution='1min', products=['BTC-USD']):
    # orders_in_time_window is used to cache what we're about to analyze
    # in RAM within a single pass. We don't want to continuously query the DB.
    orders = orders_in_time_window(start_dt, end_dt, products)
    instants = pandas.date_range(
        start=start_dt, end=end_dt, freq=resolution).tolist()
    with multiprocessing.Pool() as pool:
        params = [(orders, instant) for instant in instants]
        features = pool.starmap(_order_books_features, params)
    return features
