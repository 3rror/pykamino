import itertools
import multiprocessing
import os
from datetime import datetime
from decimal import Decimal
from itertools import repeat
from statistics import mean

import numpy as np
import pandas as pd

from pykamino.db import OrderState
from pykamino.features import TimeWindow, sliding_time_windows
from pykamino.features.decorators import rounded


def asks(orders):
    """Ask orders sorted by price."""
    ask_orders = orders[orders.side == 'ask']
    if ask_orders.empty:
        raise ValueError('No ask orders in the dataframe.')
    return ask_orders


def bids(orders):
    """Bid orders sorted by price."""
    bid_orders = orders[orders.side == 'bid']
    if bid_orders.empty:
        raise ValueError('No bid orders in the dataframe.')
    return bid_orders


def best_ask_order(orders):
    """
    Ask order with the minimimum price.
    If there are more orders with the same price, the one with the
    maximum amount is returned.
    """
    return (asks(orders)
            .sort_values(["price", "amount"], ascending=[True, False])
            .iloc[0])


def best_bid_order(orders):
    """
    Bid order with the maximum price.
    If there are more orders with the same price, the one with the
    minimum amount is returned.
    """
    return (bids(orders)
            .sort_values(["price", "amount"], ascending=[True, False])
            .iloc[-1])


def best_ask_price(orders):
    """Minimum price among ask orders."""
    return best_ask_order(orders).price


def best_bid_price(orders):
    """Maximum price among bid orders."""
    return best_bid_order(orders).price


def best_ask_amount(orders):
    """
    Total amount of assets of the ask orders at the best price.
    The best ask price is the minimum price sellers are willing to
    accept.
    """
    ask_orders = asks(orders)
    best_price_mask = ask_orders.price == best_ask_price(orders)
    return ask_orders[best_price_mask].sum().amount


def best_bid_amount(orders):
    """
    Total amount of assets of the bid orders at the best price.
    The best bid price is the maximum price buyers are willing to
    pay.
    """
    bid_orders = bids(orders)
    best_price_mask = bid_orders.price == best_bid_price(orders)
    return bid_orders[best_price_mask].sum().amount


@rounded
def mid_market_price(orders):
    """
    Mean between the best bid price and the best ask price.
    The mid market price represents an accurate estimate of the true price
    of the asset (BTC in this case) at one instant.
    """
    return mean([best_bid_price(orders), best_ask_price(orders)])


@rounded
def spread(orders):
    """
    Difference between the highest price that a buyer is willing to pay
    (bid) and the lowest price that a seller is willing to accept (ask).
    Small spreads generate a frictionless market, where trades can occur
    with no significant movement of the price.
    """
    return best_bid_price(orders) - best_ask_price(orders)


def ask_depth(orders):
    """Number of ask orders."""
    return len(asks(orders))


def bid_depth(orders):
    """Number of bid orders."""
    return len(bids(orders))


def ask_depth_chart(orders):
    ask_orders = asks(orders)
    if ask_orders.empty:
        raise ValueError('No ask orders in the dataframe.')
    return (ask_orders
            .groupby("price")
            .sum().amount
            .cumsum().reset_index())


def bid_depth_chart(orders):
    bid_orders = bids(orders)
    if bid_orders.empty:
        raise ValueError('No bid orders in the dataframe.')
    return (bid_orders
            .groupby("price")
            .sum().amount
            .iloc[::-1]
            .cumsum()
            .iloc[::-1]
            .reset_index())


def ask_depth_chart_bins(orders, bins=10):
    ask_part = ask_depth_chart(orders)
    ask_part = ask_part
    ask_part = ask_part[ask_part.price < 1.99 * float(mid_market_price(orders))]
    ask_bins = ask_part.groupby(
        pd.cut(
            ask_part.price,
            np.linspace(ask_part.price.min(), ask_part.price.max(), bins)))
    return ask_bins.mean().itertuples(index=False)


def bid_depth_chart_bins(orders, bins=10):
    bid_part = bid_depth_chart(orders)
    bid_part = bid_part
    bid_part = bid_part[bid_part.price > 0.01 * float(mid_market_price(orders))]
    bid_bins = bid_part.groupby(
        pd.cut(
            bid_part.price,
            np.linspace(bid_part.price.min(), bid_part.price.max(), bins)))
    return bid_bins.mean().itertuples(index=False)


def volume(orders):
    return orders.amount.sum()


def ask_volume(orders):
    return volume(asks(orders))


def bid_volume(orders):
    return volume(bids(orders))


@rounded
def ask_volume_weighted(orders):
    mmp = mid_market_price(orders)
    ask_orders = asks(orders)
    return ask_orders.amount.dot(ask_orders.price.subtract(mmp).rdiv(1))


@rounded
def bid_volume_weighted(orders):
    mmp = mid_market_price(orders)
    bid_orders = bids(orders)
    return bid_orders.amount.dot(bid_orders.price.subtract(mmp).rdiv(-1))


def compute_all(orders):
    """Dictionary of all the features in this order book"""

    def _ask_depth_chart_bins(orders, count):
        bins = {}
        depth_chart = ask_depth_chart_bins(orders, count+1)
        for index, point in enumerate(depth_chart):
            bins[f"ask_depth_chart_bin{index}"] = point.amount
            bins[f"ask_depth_chart_bin_price{index}"] = point.price
        return bins

    def _bid_depth_chart_bins(orders, count):
        bins = {}
        depth_chart = bid_depth_chart_bins(orders, count+1)
        for index, point in enumerate(depth_chart):
            bins[f"bid_depth_chart_bin{index}"] = point.amount
            bins[f"bid_depth_chart_bin_price{index}"] = point.price
        return bins

    return {
        "mid_market_price": mid_market_price(orders),
        "best_ask_price": best_ask_price(orders),
        "best_bid_price": best_bid_price(orders),
        "best_ask_amount": best_ask_amount(orders),
        "best_bid_amount": best_bid_amount(orders),
        "market_spread": spread(orders),
        "ask_depth": ask_depth(orders),
        "bid_depth": bid_depth(orders),
        "ask_volume": ask_volume(orders),
        "bid_volume": bid_volume(orders),
        "ask_volume_weighted": ask_volume_weighted(orders),
        "bid_volume_weighted": bid_volume_weighted(orders),
        **_ask_depth_chart_bins(orders, 10),
        **_bid_depth_chart_bins(orders, 10),
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
    return [features_from_subset(orders, w) for w in intervals]


def features_from_subset(orders, interval: TimeWindow):
    if len(orders) == 0:
        return None
    filt = ((orders.starting_at <= interval[0]) &
            ((orders.ending_at > interval[0]) | orders.ending_at.isnull()))
    orders = orders[filt].astype({'price': float, 'amount': float})
    return {**compute_all(orders), 'timestamp': interval[0]}
