import itertools
import multiprocessing
import os
from datetime import datetime
from decimal import Decimal
from functools import lru_cache
from itertools import repeat
from statistics import mean

import numpy as np
import pandas as pd

from pykamino.db import OrderState
from pykamino.features import TimeWindow, sliding_time_windows
from pykamino.features.decorators import rounded


class FeatureCalculator():
    def __init__(self, orders, timestamp):
        self._orders = self._filter_by_timestamp(orders, timestamp)
        self.timestamp = timestamp

    def _filter_by_timestamp(self, orders, timestamp):
        if len(orders) == 0:
            raise ValueError(f'No open orders at timestamp {timestamp}.')
        are_open = ((orders.starting_at <= timestamp) &
                    ((orders.ending_at > timestamp) | orders.ending_at.isnull()))
        return orders[are_open].astype({'price': float, 'amount': float})

    @lru_cache()
    def asks(self):
        """Ask orders sorted by price."""
        ask_orders = self._orders[self._orders.side == 'ask']
        if ask_orders.empty:
            raise ValueError('No ask orders in the dataframe.')
        return ask_orders

    @lru_cache()
    def bids(self):
        """Bid orders sorted by price."""
        bid_orders = self._orders[self._orders.side == 'bid']
        if bid_orders.empty:
            raise ValueError('No bid orders in the dataframe.')
        return bid_orders

    @lru_cache()
    def best_ask_order(self):
        """
        Ask order with the minimimum price.
        If there are more orders with the same price, the one with the
        maximum amount is returned.
        """
        return (self
                .asks()
                .sort_values(["price", "amount"], ascending=[True, False])
                .iloc[0])

    @lru_cache()
    def best_bid_order(self):
        """
        Bid order with the maximum price.
        If there are more orders with the same price, the one with the
        minimum amount is returned.
        """
        return (self
                .bids()
                .sort_values(["price", "amount"], ascending=[True, False])
                .iloc[-1])

    def best_ask_price(self):
        """Minimum price among ask orders."""
        return self.best_ask_order().price

    def best_bid_price(self):
        """Maximum price among bid orders."""
        return self.best_bid_order().price

    def best_ask_amount(self):
        """
        Total amount of assets of the ask orders at the best price.
        The best ask price is the minimum price sellers are willing to
        accept.
        """
        best_price_mask = self.asks().price == self.best_ask_price()
        return self.asks()[best_price_mask].sum().amount

    def best_bid_amount(self):
        """
        Total amount of assets of the bid orders at the best price.
        The best bid price is the maximum price buyers are willing to
        pay.
        """
        best_price_mask = self.asks().price == self.best_bid_price()
        return self.asks()[best_price_mask].sum().amount

    @lru_cache()
    @rounded
    def mid_market_price(self):
        """
        Mean between the best bid price and the best ask price.
        The mid market price represents an accurate estimate of the true price
        of the asset (BTC in this case) at one instant.
        """
        return mean([self.best_bid_price(), self.best_ask_price()])

    @rounded
    def spread(self):
        """
        Difference between the highest price that a buyer is willing to pay
        (bid) and the lowest price that a seller is willing to accept (ask).
        Small spreads generate a frictionless market, where trades can occur
        with no significant movement of the price.
        """
        return self.best_bid_price() - self.best_ask_price()

    @lru_cache()
    def ask_depth(self):
        """Number of ask orders."""
        return len(self.asks())

    @lru_cache()
    def bid_depth(self):
        """Number of bid orders."""
        return len(self.bids())

    @lru_cache()
    def ask_depth_chart(self):
        return (self.asks()
                .groupby("price")
                .sum().amount
                .cumsum().reset_index())

    @lru_cache()
    def bid_depth_chart(self):
        return (self.bids()
                .groupby("price")
                .sum().amount
                .iloc[::-1]
                .cumsum()
                .iloc[::-1]
                .reset_index())

    def ask_depth_chart_bins(self, bins=10):
        ask_part = self.ask_depth_chart()
        ask_part = ask_part[ask_part.price <
                            1.99 * float(self.mid_market_price())]
        ask_bins = ask_part.groupby(
            pd.cut(
                ask_part.price,
                np.linspace(ask_part.price.min(), ask_part.price.max(), bins)))
        return ask_bins.mean().itertuples(index=False)

    def bid_depth_chart_bins(self, bins=10):
        bid_part = self.bid_depth_chart()
        bid_part = bid_part[bid_part.price >
                            0.01 * float(self.mid_market_price())]
        bid_bins = bid_part.groupby(
            pd.cut(
                bid_part.price,
                np.linspace(bid_part.price.min(), bid_part.price.max(), bins)))
        return bid_bins.mean().itertuples(index=False)

    def _volume(self, orders):
        return orders.amount.sum()

    def ask_volume(self):
        return self._volume(self.asks())

    def bid_volume(self):
        return self._volume(self.bids())

    @lru_cache()
    @rounded
    def ask_volume_weighted(self):
        mmp = self.mid_market_price()
        return self.asks().amount.dot(self.asks().price.subtract(mmp).rdiv(1))

    @lru_cache()
    @rounded
    def bid_volume_weighted(self):
        mmp = self.mid_market_price()
        return self.bids().amount.dot(self.bids().price.subtract(mmp).rdiv(-1))

    def compute_all(self):
        """Dictionary of all the features in this order book"""

        def _ask_depth_chart_bins(count):
            bins = {}
            depth_chart = self.ask_depth_chart_bins(count+1)
            for index, point in enumerate(depth_chart):
                bins[f"ask_depth_chart_bin{index}"] = point.amount
                bins[f"ask_depth_chart_bin_price{index}"] = point.price
            return bins

        def _bid_depth_chart_bins(count):
            bins = {}
            depth_chart = self.bid_depth_chart_bins(count+1)
            for index, point in enumerate(depth_chart):
                bins[f"bid_depth_chart_bin{index}"] = point.amount
                bins[f"bid_depth_chart_bin_price{index}"] = point.price
            return bins

        return {
            "mid_market_price": self.mid_market_price(),
            "best_ask_price": self.best_ask_price(),
            "best_bid_price": self.best_bid_price(),
            "best_ask_amount": self.best_ask_amount(),
            "best_bid_amount": self.best_bid_amount(),
            "market_spread": self.spread(),
            "ask_depth": self.ask_depth(),
            "bid_depth": self.bid_depth(),
            "ask_volume": self.ask_volume(),
            "bid_volume": self.bid_volume(),
            "ask_volume_weighted": self.ask_volume_weighted(),
            "bid_volume_weighted": self.bid_volume_weighted(),
            **_ask_depth_chart_bins(10),
            **_bid_depth_chart_bins(10),
        }


def fetch_orders(interval, product='BTC-USD'):
    orders = (OrderState
              .select(
                  OrderState.side, OrderState.price, OrderState.amount, OrderState.starting_at, OrderState.ending_at)
              .where(
                    (OrderState.product == product) &
                    (OrderState.starting_at <= interval.end) &
                    (
                        (OrderState.ending_at > interval.start) |
                        (OrderState.ending_at == None)
                    )
              )
              .namedtuples())

    return pd.DataFrame(orders)


def extract(interval: TimeWindow, res='10min', stride=10, products=('BTC-USD')):
    features = {}
    res = pd.to_timedelta(res)
    with multiprocessing.Pool() as pool:
        for product in products:
            output = pool.imap(_extract,
                               sliding_time_windows(interval, res, stride=100, chunksize=25))
            features[product] = list(itertools.chain(*output))
    return features['BTC-USD']


def _extract(intervals):
    range = TimeWindow(intervals[0].start, intervals[-1].end)
    orders = fetch_orders(range)
    return [features_from_subset(orders, w.start) for w in intervals]


def features_from_subset(orders, instant):
    if len(orders) == 0:
        return None
    fc = FeatureCalculator(orders, instant)
    return {**fc.compute_all(), 'timestamp': instant}
