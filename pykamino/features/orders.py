import itertools
import multiprocessing
from statistics import mean

import pandas as pd

from pykamino.db import OrderState
from pykamino.features import TimeWindow, sliding_time_windows
from pykamino.features.decorators import rounded


def memoize(func):
    """
    Cache the output of the function. Very useful to speed up multiple calls of
    computational intensive functions.

    Defines, if not present, an instance variable called _cache.
    """
    def wrapper(self, *args, **kwargs):
        if not hasattr(self, '_cache'):
            self._cache = {}
        if func not in self._cache:
            self._cache[func] = func(self, *args, **kwargs)
        return self._cache[func]
    return wrapper


class FeatureCalculator():
    def __init__(self, orders, timestamp):
        self._orders = self._open_orders_at_timestamp(orders, timestamp)
        self.timestamp = timestamp

    def _open_orders_at_timestamp(self, orders, timestamp):
        if len(orders) == 0:
            raise ValueError(f'No open orders at timestamp {timestamp}.')
        condition = (
            (orders.starting_at <= timestamp) &
            ((orders.ending_at > timestamp) | orders.ending_at.isnull()))
        open_orders = orders[condition]
        # Some pandas functions, like groupby(), drop fields that are "Objects"
        # (i.e. custom fields, Decimals...). Because of this, we pre-cast price
        # and amount to floats.
        return open_orders.astype({'price': float, 'amount': float})

    @memoize
    def asks(self):
        """Ask orders sorted by price."""
        ask_orders = self._orders[self._orders.side == 'ask']
        if ask_orders.empty:
            raise ValueError('No ask orders in the dataframe.')
        return ask_orders

    @memoize
    def bids(self):
        """Bid orders sorted by price."""
        bid_orders = self._orders[self._orders.side == 'bid']
        if bid_orders.empty:
            raise ValueError('No bid orders in the dataframe.')
        return bid_orders

    @memoize
    def best_ask_order(self):
        """
        Ask order with the minimum price.
        If there are more orders with the same price, the one with the maximum
        amount is returned.
        """
        return (
            self
            .asks()
            .sort_values(["price", "amount"], ascending=[True, False])
            .iloc[0])

    @memoize
    def best_bid_order(self):
        """
        Bid order with the maximum price.
        If there are more orders with the same price, the one with the minimum
        amount is returned.
        """
        return (
            self
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
        The best ask price is the minimum price sellers are willing to accept.
        """
        best_price_mask = self.asks().price == self.best_ask_price()
        return self.asks()[best_price_mask].sum().amount

    def best_bid_amount(self):
        """
        Total amount of assets of the bid orders at the best price.
        The best bid price is the maximum price buyers are willing to pay.
        """
        best_price_mask = self.bids().price == self.best_bid_price()
        return self.bids()[best_price_mask].sum().amount

    @memoize
    @rounded
    def mid_market_price(self):
        """
        Mean between the best bid price and the best ask price.
        The mid market price represents an accurate estimate of the true price
        of the asset (BTC in this case) at one instant.
        """
        return mean([self.best_bid_price(), self.best_ask_price()])

    @rounded
    def bid_ask_spread(self):
        """
        Difference between the highest price that a buyer is willing to pay
        (bid) and the lowest price that a seller is willing to accept (ask).
        Small spreads generate a frictionless market, where trades can occur
        with no significant movement of the price.
        """
        return self.best_bid_price() - self.best_ask_price()

    @memoize
    def ask_depth(self):
        """Number of ask orders."""
        return len(self.asks())

    @memoize
    def bid_depth(self):
        """Number of bid orders."""
        return len(self.bids())

    @memoize
    def chart(self):
        chart = pd.concat([self._bids_chart(), self._asks_chart()])
        no_outliners_filter = (
            (chart.price < 1.99 * self.mid_market_price()) &
            (chart.price > 0.01 * self.mid_market_price()))
        return chart[no_outliners_filter]

    def sampled_chart(self, bins=30):
        return (
            self.chart()
            .groupby(pd.cut(self.chart().price, bins), sort=False)
            .mean().amount
            .tolist())

    @memoize
    def _bids_chart(self):
        return (
            self.bids()
            .groupby("price")
            .sum().amount
            .iloc[::-1]
            .cumsum()
            .iloc[::-1]
            .reset_index())

    @memoize
    def _asks_chart(self):
        return (
            self.asks()
            .groupby("price")
            .sum().amount
            .cumsum().reset_index())

    def _volume(self, orders):
        return orders.amount.sum()

    def ask_volume(self):
        return self._volume(self.asks())

    def bid_volume(self):
        return self._volume(self.bids())

    @memoize
    @rounded
    def ask_volume_weighted(self):
        mmp = self.mid_market_price()
        return self.asks().amount.dot(self.asks().price.subtract(mmp).rdiv(1))

    @memoize
    @rounded
    def bid_volume_weighted(self):
        mmp = self.mid_market_price()
        return self.bids().amount.dot(self.bids().price.subtract(mmp).rdiv(-1))

    def compute_all(self):
        """Dictionary of all the features in this order book"""
        all_features = ["mid_market_price", "best_ask_price", "best_bid_price",
                        "best_ask_amount", "best_bid_amount",
                        "bid_ask_spread", "ask_depth", "bid_depth",
                        "ask_volume", "bid_volume", "ask_volume_weighted",
                        "bid_volume_weighted", "sampled_chart"]
        output = {}
        for feature_name in all_features:
            output[feature_name] = getattr(self, feature_name)()
        return output


def fetch_states(interval, product='BTC-USD'):
    orders = (
        OrderState
        .select(
            OrderState.side, OrderState.price, OrderState.amount,
            OrderState.starting_at, OrderState.ending_at)
        .where(
            (OrderState.product == product) &
            (OrderState.starting_at <= interval.end) &
            ((OrderState.ending_at > interval.start) |
             (OrderState.ending_at.is_null())))
        .namedtuples())
    return pd.DataFrame(orders)


def extract(interval: TimeWindow, res='2min', products=('BTC-USD',)):
    features = {}
    res = pd.to_timedelta(res)
    with multiprocessing.Pool() as pool:
        for product in products:
            windows = sliding_time_windows(interval, res, stride=100,
                                           chunksize=50)
            output = pool.imap(_extract, windows)
            features[product] = list(itertools.chain(*output))
    # TODO: Support multiple currencies. For now we consider only BTC
    return features['BTC-USD']


def _extract(intervals):
    range = TimeWindow(intervals[0].start, intervals[-1].end)
    orders = fetch_states(range)
    instants = [i.start for i in intervals]
    instants.append(intervals[-1].end)
    return [features_from_subset(orders, i) for i in instants]


def features_from_subset(orders, instant):
    if len(orders) == 0:
        return None
    fc = FeatureCalculator(orders, instant)
    return {**fc.compute_all(), 'timestamp': instant}
