import os
from datetime import datetime
from itertools import repeat
import multiprocessing
from statistics import mean
from decimal import Decimal

import numpy as np
import pandas
from pykamino.db import Order as O
from pykamino.db import OrderHistory as Oh
from pykamino.features.decorators import rounded


def asks(orders):
    """Ask orders sorted by price."""
    return orders[orders.side == 'ask']


def bids(orders):
    """Bid orders sorted by price."""
    return orders[orders.side == "bid"]


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
    best_price_mask = asks(orders).price == best_ask_price(orders)
    return asks(orders)[best_price_mask].sum().amount


def best_bid_amount(orders):
    """
    Total amount of assets of the bid orders at the best price.
    The best bid price is the maximum price buyers are willing to
    pay.
    """
    best_price_mask = bids(orders).price == best_bid_price(orders)
    return bids(orders)[best_price_mask].sum().amount


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
    return (asks(orders)
            .sort_values(by="price")
            .groupby("price")
            .sum().amount
            .cumsum().reset_index())


def bid_depth_chart(orders):
    return (bids(orders)
            .sort_values(by="price")
            .groupby("price")
            .sum().amount
            .iloc[::-1]
            .cumsum()
            .iloc[::-1]
            .reset_index())


def ask_depth_chart_bins(orders, bins=10):
    ask_part = ask_depth_chart(orders)
    ask_part = ask_part[ask_part.price < Decimal(
        '1.99') * mid_market_price(orders)]
    ask_bins = ask_part.groupby(
        pandas.cut(
            ask_part.price,
            np.linspace(ask_part.price.min(), ask_part.price.max(), bins),
        )
    )
    ask_samples = ask_bins.mean().amount.tolist()
    return ask_samples


def bid_depth_chart_bins(orders, bins=10):
    bid_part = bid_depth_chart(orders)
    bid_part = bid_part[bid_part.price > Decimal(
        '0.01') * orders.mid_market_price()]
    bid_bins = bid_part.groupby(
        pandas.cut(
            bid_part.price,
            np.linspace(bid_part.price.min(), bid_part.price.max(), bins),
        )
    )
    bid_samples = bid_bins.mean().iloc[::-1].amount.tolist()
    return bid_samples


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
        return {
            f"ask_depth_chart_bin{i}": ask_depth_chart_bins(orders, count+1)[i]
            for i in range(count)
        }

    def _bid_depth_chart_bins(orders, count):
        return {
            f"bid_depth_chart_bin{i}": bid_depth_chart_bins(orders, count+1)[i]
            for i in range(count)
        }

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


def select_orders(start_dt, end_dt, products):
    orders = (
        list(O.select(O.id, O.side, O.price, O.close_time, Oh.time, Oh.amount)
             .join(Oh)
             .where((Oh.time <= end_dt) &
                    ((O.close_time > start_dt) | (O.close_time == None)) &
                    O.product.in_(products))
             .order_by(Oh.time).dicts()))

    return pandas.DataFrame(orders)


def order_book_from_cache(orders, instant):
    filt = ((orders.time <= instant) &
            ((orders.close_time > instant) | orders.close_time.isnull()))
    # Careful! This expects the orders to be sorted by orders' insertion time
    return orders[filt].drop_duplicates(subset='id', keep='last')


def features_in_subset(orders, instant):
    order_book = order_book_from_cache(orders, instant)
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
