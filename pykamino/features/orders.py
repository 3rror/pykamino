import os
from datetime import datetime
from functools import lru_cache as cache
from itertools import repeat
import multiprocessing
from statistics import mean
from decimal import Decimal

import numpy as np
import pandas
from pykamino.db import Order as O
from pykamino.db import OrderHistory as Oh
from pykamino.features.decorators import rounded


@cache(maxsize=1)
def asks(df):
    """Ask orders ordered by price."""
    return df[df.side == 'ask']


@cache(maxsize=1)
def bids(df):
    """Bid orders ordered by price."""
    return df[df.side == "bid"]


@cache(maxsize=1)
def best_ask_order(df):
    """
    Ask order with the minimimum price.
    If there are more orders with the same price, the one with the
    maximum amount is returned.
    """
    return (asks(df)
            .sort_values(["price", "size"], ascending=[True, False])
            .iloc[0])


@cache(maxsize=1)
def best_bid_order(df):
    """
    Bid order with the maximum price.
    If there are more orders with the same price, the one with the
    minimum amount is returned.
    """
    return (bids(df)
            .sort_values(["price", "size"], ascending=[True, False])
            .iloc[-1])


@cache(maxsize=1)
def best_ask_price(df):
    """Minimum price among ask orders."""
    return asks(df).price.min()


@cache(maxsize=1)
def best_bid_price(df):
    """Maximum price among bid orders."""
    return bids(df).price.max()


@cache(maxsize=1)
def best_ask_amount(df):
    """
    Total amount of assets of the ask orders at the best price.
    The best ask price is the minimum price sellers are willing to
    accept.
    """
    best_price_mask = asks(df).price == best_ask_price(df)
    return asks(df)[best_price_mask].sum().amount


@cache(maxsize=1)
def best_bid_amount(df):
    """
    Total amount of assets of the bid orders at the best price.
    The best bid price is the maximum price buyers are willing to
    pay.
    """
    best_price_mask = bids(df).price == best_bid_price(df)
    return bids(df)[best_price_mask].sum().amount


@cache(maxsize=1)
@rounded
def mid_market_price(df):
    """
    Mean between the best bid price and the best ask price.
    The mid market price represents an accurate estimate of the true price
    of the asset (BTC in this case) at one instant.
    """
    return mean([best_bid_price(df), best_ask_price(df)])


@rounded
def spread(df):
    """
    Difference between the highest price that a buyer is willing to pay
    (bid) and the lowest price that a seller is willing to accept (ask).
    Small spreads generate a frictionless market, where trades can occur
    with no significant movement of the price.
    """
    return best_bid_price(df) - best_ask_price(df)


def ask_depth(df):
    """Number of ask orders."""
    return len(asks(df))


def bid_depth(df):
    """Number of bid orders."""
    return len(bids(df))


@cache(maxsize=1)
def ask_depth_chart(df):
    return (asks(df)
            .sort_values(by="price")
            .groupby("price")
            .sum().amount
            .cumsum().reset_index())


@cache(maxsize=1)
def bid_depth_chart(df):
    return (bids(df)
            .sort_values(by="price")
            .groupby("price")
            .sum().amount
            .iloc[::-1]
            .cumsum()
            .iloc[::-1]
            .reset_index())


@cache(maxsize=1)
def ask_depth_chart_bins(df, bins=10):
    ask_part = ask_depth_chart(df)
    ask_part = ask_part[ask_part.price < Decimal(
        '1.99') * mid_market_price(df)]
    ask_bins = ask_part.groupby(
        pandas.cut(
            ask_part.price,
            np.linspace(ask_part.price.min(), ask_part.price.max(), bins),
        )
    )
    ask_samples = ask_bins.mean().amount.tolist()
    return ask_samples


@cache(maxsize=1)
def bid_depth_chart_bins(df, bins=10):
    bid_part = bid_depth_chart(df)
    bid_part = bid_part[bid_part.price > Decimal(
        '0.01') * df.mid_market_price()]
    bid_bins = bid_part.groupby(
        pandas.cut(
            bid_part.price,
            np.linspace(bid_part.price.min(), bid_part.price.max(), bins),
        )
    )
    bid_samples = bid_bins.mean().iloc[::-1].amount.tolist()
    return bid_samples


def _volume_weighted_by_price(df, price_weight=1):
    mid_price = mid_market_price(df)
    return df.amount.dot(
        df.price.subtract(mid_price).apply(
            lambda x: abs(1 / -x) ** price_weight
        )
    )


@cache(maxsize=2)
def ask_volume_weighted(df, price_weight=1):
    return _volume_weighted_by_price(asks(df), price_weight)


@cache(maxsize=2)
def bid_volume_weighted(df, price_weight=1):
    return _volume_weighted_by_price(bids(df), price_weight)


def compute_all(df):
    """Dictionary of all the features in this order book"""

    def _ask_depth_chart_bins(df, count):
        return {
            f"ask_depth_chart_bin{i}": ask_depth_chart_bins(df, count+1)[i]
            for i in range(count)
        }

    def _bid_depth_chart_bins(df, count):
        return {
            f"bid_depth_chart_bin{i}": bid_depth_chart_bins(df, count+1)[i]
            for i in range(count)
        }

    return {
        "mid_market_price": mid_market_price(df),
        "best_ask_price": best_ask_price(df),
        "best_bid_price": best_bid_price(df),
        "best_ask_amount": best_ask_amount(df),
        "best_bid_amount": best_bid_amount(df),
        "market_spread": spread(df),
        "ask_depth": ask_depth(df),
        "bid_depth": bid_depth(df),
        "ask_volume": ask_volume_weighted(df, price_weight=0),
        "bid_volume": bid_volume_weighted(df, price_weight=0),
        "ask_volume_weighted": ask_volume_weighted(df),
        "bid_volume_weighted": bid_volume_weighted(df),
        **_ask_depth_chart_bins(df, 10),
        **_bid_depth_chart_bins(df, 10),
    }


def select_orders(start_dt, end_dt, products):
    orders = (
        list(O.select(O.id, O.side, O.price, O.close_time, Oh.time, Oh.amount)
             .join(Oh)
             .where((Oh.time <= end_dt) &
                    ((O.close_time > start_dt) | (O.close_time == None)) &
                    O.product.in_(products))
             .order_by(Oh.time).dicts()))

    return pandas.DataFrame(orders)


def order_book_from_cache(df, instant):
    filt = ((df.time <= instant) &
            ((df.close_time > instant) | df.close_time.isnull()))
    # Careful! This expects the DF to be sorted by orders' insertion time
    return df[filt].drop_duplicates(subset='id', keep='last')


def features_in_subset(df, instant):
    order_book = order_book_from_cache(df, instant)
    if len(order_book) == 0:
        return None
    return compute_all(order_book)


def extract(start_dt, end_dt, resolution='1min', products=['BTC-USD']):
    # orders_in_time_window is used to cache what we're about to analyze
    # in RAM within a single pass. We don't want to continuously query the DB.
    order_cache = select_orders(start_dt, end_dt, products)
    windows = pandas.date_range(start=start_dt, end=end_dt,
                                freq=resolution).tolist()
    with multiprocessing.Pool() as pool:
        features = pool.starmap(features_in_subset,
                                zip(repeat(order_cache), windows))
    features = [f for f in features if f is not None]
    return features
