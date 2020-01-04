from datetime import datetime
from functools import partial
from typing import List, Tuple
import itertools
import multiprocessing

from pykamino.db import OrderState
from pykamino.features import TimeWindow, sliding_time_windows
from pykamino.features.decorators import rounded
import numpy
import pandas

FEATURES = ('mid_market_price', 'best_ask_price', 'best_bid_price', 'best_ask_amount',
            'best_bid_amount', 'bid_ask_spread', 'ask_depth', 'bid_depth', 'ask_volume',
            'bid_volume', 'ask_volume_weighted', 'bid_volume_weighted', 'sampled_chart')


def asks(orders: pandas.DataFrame) -> pandas.DataFrame:
    """
    Get orders of type "ask".

    Args:
        orders: dataFrame of orders
    """
    return orders[orders.side == 'ask']


def bids(orders: pandas.DataFrame) -> pandas.DataFrame:
    """
    Get orders of type "bid".

    Args:
        orders: dataFrame of orders
    """
    return orders[orders.side == 'bid']


def best_ask_order(orders: pandas.DataFrame) -> pandas.Series:
    """
    Get the ask order with the minimum price.
    If there are more orders with the same price, the one with the maximum
    amount is returned.

    Args:
        orders: dataFrame of orders
    """
    # DataFrames are mutable, thus not hashable. For this reason we cannot make use
    # of memoization but resort to such a hacky and stupid local-scoped cache.
    sks = asks(orders)
    index = sks[sks.price == sks.price.min()]['amount'].idxmax()
    return sks.loc[index]


def best_bid_order(orders: pandas.DataFrame) -> pandas.Series:
    """
    Get the bid order with the maximum price.
    If there are more orders with the same price, the one with the minimum
    amount is returned.

    Args:
        orders: dataFrame of orders
    """
    bds = bids(orders)
    index = bds[bds.price == bds.price.max()]['amount'].idxmin()
    return bds.loc[index]


def best_ask_price(orders: pandas.DataFrame):
    """
    Get the minimum price among ask orders.

    Args:
        orders: dataFrame of orders
    """
    return best_ask_order(orders).price


def best_bid_price(orders: pandas.DataFrame):
    """
    Get the maximum price among ask orders.

    Args:
        orders: dataFrame of orders
    """
    return best_bid_order(orders).price


def best_ask_amount(orders: pandas.DataFrame):
    """
    Ge the total amount of assets for the ask orders at the best price.
    The best ask price is the minimum price that sellers are willing to take.

    Args:
        orders: dataFrame of orders
    """
    sks = asks(orders)
    best_price_mask = sks.price == best_ask_price(orders)
    return sks[best_price_mask].sum().amount


def best_bid_amount(orders: pandas.DataFrame):
    """
    Get the total amount of assets for the bid orders at the best price.
    The best bid price is the maximum price buyers are willing to pay.

    Args:
        orders: dataFrame of orders
    """
    bds = bids(orders)
    best_price_mask = bds.price == best_bid_price(orders)
    return bds[best_price_mask].sum().amount


@rounded
def mid_market_price(orders: pandas.DataFrame):
    """
    Get the mean between the best bid price and the best ask price.
    The mid market price represents an accurate estimate of the true price
    of the asset (BTC, or ETH, etc.) at a given instant.

    Args:
        orders: dataFrame of orders
    """
    return numpy.mean((best_bid_price(orders), best_ask_price(orders)))


@rounded
def bid_ask_spread(orders: pandas.DataFrame):
    """
    Get the difference between the highest price that a buyer is willing to pay
    (bid) and the lowest price that a seller is willing to accept (ask).
    Small spreads generate a frictionless market, in which trades can occur
    with no significant movement of the price.

    Args:
        orders: dataFrame of orders
    """
    return best_bid_price(orders) - best_ask_price(orders)


def ask_depth(orders: pandas.DataFrame) -> int:
    """
    Get the number of ask orders.

    Args:
        orders: dataFrame of orders
    """
    return len(asks(orders))


def bid_depth(orders: pandas.DataFrame) -> int:
    """
    Get the number of bid orders.

    Args:
        orders: dataFrame of orders
    """
    return len(bids(orders))


def chart(orders):
    chart = pandas.concat([bids_chart(orders), asks_chart(orders)])
    no_outliners_filter = (
        (chart.price < 1.99 * mid_market_price(orders)) &
        (chart.price > 0.01 * mid_market_price(orders)))
    return chart[no_outliners_filter]


def sampled_chart(orders, bins=30):
    return (chart(orders)
            .groupby(pandas.cut(chart(orders).price, bins), sort=False)
            .mean().amount
            .tolist())


def bids_chart(orders):
    return (
        bids(orders)
        .groupby('price')
        .sum().amount
        .iloc[::-1]
        .cumsum()
        .iloc[::-1]
        .reset_index())


def asks_chart(orders):
    return (
        asks(orders)
        .groupby('price')
        .sum().amount
        .cumsum().reset_index())


def ask_volume(states: pandas.DataFrame):
    """
    Get the total amount of assets for "ask" orders.

    Args:
        orders: dataFrame of orders
    """
    return asks(states).amount.sum()


def bid_volume(states: pandas.DataFrame):
    """
    Get the total amount of assets for "bid" orders.

    Args:
        orders: dataFrame of orders
    """
    return bids(states).amount.sum()


@rounded
def ask_volume_weighted(states: pandas.DataFrame):
    sks = asks(states)
    return sks.amount.dot(sks.price.subtract(mid_market_price(states)).rdiv(1))


@rounded
def bid_volume_weighted(states: pandas.DataFrame):
    bds = bids(states)
    return bds.amount.dot(bds.price.subtract(mid_market_price(states)).rdiv(-1))


def fetch_states(interval: TimeWindow, product: str = 'BTC-USD') -> pandas.DataFrame:
    """
    Get a pandas.DataFrame of all the order states in the time window. Only the open states
    will be fetched.

    Args:
        interval: time window from which to fetch states
        product: currency to consider

    Returns:
        order states in `interval`
    """
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
    return pandas.DataFrame(orders, dtype=numpy.float64)


def get_open_orders(order_states: pandas.DataFrame, instant: datetime):
    open_condition = (
        (order_states.starting_at <= instant) &
        ((order_states.ending_at > instant) | order_states.ending_at.isnull()))
    return order_states[open_condition]


def extraction_worker(intervals: List[TimeWindow], product: str = 'BTC-USD'):
    def compute(orders, instant):
        """
        Take a big dataframe and compute features only for a certain time interval.
        """
        open_orders = get_open_orders(orders, instant)
        feats = {'timestamp': instant}
        module_scope = globals()
        for f in FEATURES:
            feats[f] = module_scope[f](open_orders)
        return feats

    range = TimeWindow(intervals[0].start, intervals[-1].end)
    orders = fetch_states(range, product=product)
    instants = itertools.chain(
        (i.start for i in intervals), (intervals[-1].end,))
    return [compute(orders, i) for i in instants]


def extract(interval: TimeWindow, res: str = '2min', products: Tuple[str, ...] = ('BTC-USD',)):
    res = pandas.to_timedelta(res)
    with multiprocessing.Pool() as pool:
        for product in products:
            # TODO: chunksize=200 is good for a 1-second resolution, so that computation time exceeds
            # query time, but ideally the chunksize is adaptive.
            windows = sliding_time_windows(
                interval, res, stride=100, chunksize=200)
            worker = partial(extraction_worker, product=product)
            feat_lists = pool.imap(worker, windows, chunksize=2)
            yield product, itertools.chain(*feat_lists)
