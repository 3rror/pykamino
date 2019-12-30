from decimal import Decimal
from functools import partial
from typing import Any, Dict, List, Optional, Tuple
import multiprocessing

from pykamino.db import database, Trade
from pykamino.features import TimeWindow, sliding_time_windows
from pykamino.features.decorators import rounded
import numpy
import pandas


def buys(trades: pandas.DataFrame) -> pandas.DataFrame:
    """
    Get trades of type "buy".

    Args:
        trades: dataFrame of trades
    """
    return trades[trades.side == 'buy']


def sells(trades: pandas.DataFrame) -> pandas.DataFrame:
    """
    Get trades of type "sell".

    Args:
        trades: dataFrame of trades
    """
    return trades[trades.side == 'sell']


def latest_trade(trades: pandas.DataFrame) -> pandas.Series:
    """
    Get the most recent trade.

    Args:
        trades: dataFrame of trades

    Raises:
        ValueError: if `trades` is empty
    """
    return trades.loc[trades.time.idxmax()]


def oldest_trade(trades) -> pandas.Series:
    """
    Get the oldest trade in the pandas.DataFrame.

    Args:
        trades: dataFrame of trades

    Raises:
        ValueError: if `trades` is empty
    """
    return trades.loc[trades.time.idxmin()]


@rounded
def mean_price(trades: pandas.DataFrame) -> numpy.float64:
    """
    Get the mean price of all the trades.

    Args:
        trades: dataFrame of trades
    """
    return trades.price.mean()


@rounded
def price_std(trades: pandas.DataFrame) -> numpy.float64:
    """
    Get the standard deviation of the price.

    Args:
        trades: dataFrame of trades
    """
    return trades.price.astype(float).std()


def buy_count(trades: pandas.DataFrame) -> int:
    """
    Get the number of "buy" trades.

    Args:
        trades: dataFrame of trades
    """
    return len(buys(trades))


def sell_count(trades: pandas.DataFrame) -> int:
    """
    Get the number of "sell" trades.

    Args:
        trades: dataFrame of trades
    """
    return len(sells(trades))


@rounded
def total_buy_volume(trades: pandas.DataFrame) -> numpy.float64:
    """
    Get the total amount of crypto bought.

    Args:
        trades: dataFrame of trades
    """
    return buys(trades).amount.sum()


@rounded
def total_sell_volume(trades: pandas.DataFrame) -> numpy.float64:
    """
    Get the total amount of crypto sold.

    Args:
        trades: dataFrame of trades
    """
    return sells(trades).amount.sum()


@rounded
def price_movement(trades: pandas.DataFrame) -> Optional[Decimal]:
    """
    Get the price difference between the oldest trade and the most recent one.

    Args:
        trades: dataFrame of trades

    Raises:
        ValueError: if `trades` is empty
    """
    return oldest_trade(trades).price - latest_trade(trades).price


def fetch_trades(interval: TimeWindow, product: str = 'BTC-USD'):
    """
    Get a pandas.DataFrame of all the trades in the specified time window.

    Args:
        interval: time window from which to fetch trades
        product: currency to consider

    Returns:
        trades in the specified time window
    """
    trades = (Trade
              .select()
              .where((Trade.product == product) &
                     Trade.time.between(*interval)).namedtuples())
    return pandas.DataFrame(trades)


def compute_all_features(trades, interval: TimeWindow):
    try:
        trades_slice = trades[trades.time.between(*interval)]
        return {'buy_count': buy_count(trades_slice),
                'sell_count': sell_count(trades_slice),
                'total_buy_volume': total_buy_volume(trades_slice),
                'total_sell_volume': total_sell_volume(trades_slice),
                'price_mean': mean_price(trades_slice),
                'price_std': price_std(trades_slice),
                'price_movement': price_movement(trades_slice),
                'start_time': interval.start,
                'end_time': interval.end}
    except (AttributeError, ValueError):
        return {'buy_count': None,
                'sell_count': None,
                'total_buy_volume': None,
                'total_sell_volume': None,
                'price_mean': None,
                'price_std': None,
                'price_movement': None,
                'start_time': interval.start,
                'end_time': interval.end}


def extraction_worker(intervals: List[TimeWindow], product='BTC-USD'):
    range = TimeWindow(intervals[0].start, intervals[-1].end)
    trades = fetch_trades(range, product=product)
    return [compute_all_features(trades, w) for w in intervals]


def extract(interval: TimeWindow, res: str = '2min', stride: int = 10,
            products: Tuple[str, ...] = ('BTC-USD',)) -> Dict[str, List[Dict[str, Any]]]:
    """
    Extract all the trade features, fetching data from the database.
    The features will be computed for every time period of length `res`.
    Args:
        interval: a time range
        res: the feature "resolution", that is the length of a time window
        stride: overlap of a window from the preceding one
        products: some "fiat-crypto" couples of which to compute features

    Returns:
        A dictionary whose keys are `products` and values are a list of another dicts.
        Those dicts have feature names as keys.
    """
    features = {}
    res = pandas.to_timedelta(res)
    with multiprocessing.Pool() as pool:
        for product in products:
            worker = partial(extraction_worker, product=product)
            # Trades don't require much memory, we can affort to use map()
            features[product] = pool.map(worker,
                                         sliding_time_windows(interval, res, stride))
    return features
