import itertools
import multiprocessing

import pandas

from pykamino.db import Trade
from pykamino.features import TimeWindow, sliding_time_windows
from pykamino.features.decorators import rounded

# Feature calcutation #


def _buys(trades):
    """Return trades of type "buy" in the specified dataframe.

    Args:
        trades (pandas.DataFrame): dataframe of trades

    Returns:
        pandas.DataFrame: trades of type "buy"
    """
    return trades[trades.side == 'buy']


def _sells(trades):
    """Return trades of type "sell" in the specified dataframe.

    Args:
        trades (pandas.DataFrame): dataframe of trades

    Returns:
        pandas.DataFrame: trades of type "sell"
    """
    return trades[trades.side == 'sell']


def _latest_trade(trades):
    """Return the most recent trade in the dataframe.

    Args:
        trades (pandas.DataFrame): dataframe of trades

    Returns:
        [pandas.Series]: most recent trade
    """
    return trades.loc[trades.time.idxmax()]


def _oldest_trade(trades):
    """Return the oldest trade in the dataframe.

    Args:
        trades (pandas.DataFrame): dataframe of trades

    Returns:
        [pandas.Series]: oldest trade
    """
    return trades.loc[trades.time.idxmin()]


@rounded
def mean_price(trades):
    """Return the mean price of all the trades in the dataframe.

    Args:
        trades (pandas.DataFrame): dataframe of trades

    Raises:
        ValueError: raised when trades dataframe is empty

    Returns:
        numpy.float64: mean price, rounded to 8 digits
    """
    if trades.empty:
        raise ValueError(
            'Cannot calculate the mean price on an empty dataframe.')
    return trades.price.mean()


@rounded
def price_std(trades):
    """Return the standard deviation of the prices of the trades.

    Args:
        trades (pandas.DataFrame): dataframe of trades

    Raises:
        ValueError: raised when trades dataframe is empty

    Returns:
        numpy.float64: standard deviation, rounded to 8 digits
    """
    if trades.empty:
        raise ValueError(
            'Cannot calculate price standard deviation on an empty dataframe.')
    return trades.price.astype(float).std()


def buy_count(trades):
    """Return the number of "buy" trades in the dataframe.

    Args:
        trades (pandas.DataFrame): dataframe of trades

    Returns:
        int: number of "buy" trades
    """
    return len(_buys(trades))


def sell_count(trades):
    """Return the number of "sell" trades in the dataframe.

    Args:
        trades (pandas.DataFrame): dataframe of trades

    Returns:
        int: number of "sell" trades
    """
    return len(_sells(trades))


@rounded
def total_buy_volume(trades):
    """Return the total amount bought in the dataframe.

    Args:
        trades (pandas.DataFrame): dataframe of trades

    Returns:
        numpy.float64: total amount bought
    """
    return _buys(trades).amount.sum()


@rounded
def total_sell_volume(trades):
    """Return the total amount sold in the dataframe.

    Args:
        trades (pandas.DataFrame): dataframe of trades

    Returns:
        numpy.float64: total amount sold
    """
    return _sells(trades).amount.sum()


@rounded
def price_movement(trades):
    """Return the difference between the oldest and the most recent trade
    price in the dataframe.

    Args:
        trades (pandas.DataFrame): dataframe of trades

    Raises:
        ValueError: raised when trades dataframe is empty

    Returns:
        decimal.Decimal: price movement, rounded to 8 digits
    """
    if trades.empty:
        raise ValueError(
            'Cannot calculate price movement on an empty dataframe.')
    return _oldest_trade(trades).price - _latest_trade(trades).price


# Feature extraction from database #

def fetch_trades(interval: TimeWindow, product='BTC-USD'):
    """Return a dataframe of all the orders in the specified time window.

    Args:
        interval (TimeWindow): time window from which to fetch trades
        product (str, optional): currency to consider. Defaults to "BTC-USD"

    Returns:
        pandas.DataFrame: orders in the specified time window
    """
    trades = (Trade
              .select()
              .where((Trade.product == product) &
                     Trade.time.between(*interval))
              .namedtuples())
    return pandas.DataFrame(trades)


def features_from_subset(trades, interval: TimeWindow):
    """
    TODO: Add doc
    """
    try:
        trades_slice = trades[trades.time.between(*interval)]
        return {
            'buy_count': buy_count(trades_slice),
            'sell_count': sell_count(trades_slice),
            'total_buy_volume': total_buy_volume(trades_slice),
            'total_sell_volume': total_sell_volume(trades_slice),
            'price_mean': mean_price(trades_slice),
            'price_std': price_std(trades_slice),
            'price_movement': price_movement(trades_slice),
            'start_time': interval.start,
            'end_time': interval.end
        }
    except (ValueError, AttributeError):
        return {
            'buy_count': 0,
            'sell_count': 0,
            'total_buy_volume': 0,
            'total_sell_volume': 0,
            'price_mean': None,
            'price_std': None,
            'price_movement': None,
            'start_time': interval.start,
            'end_time': interval.end
        }


def extract(interval: TimeWindow, res='10min', stride=10, products=('BTC-USD')):
    features = {}
    res = pandas.to_timedelta(res)
    with multiprocessing.Pool() as pool:
        for product in products:
            output = pool.imap(_extract,
                               sliding_time_windows(interval, res, stride))
            features[product] = list(itertools.chain(*output))
    return features['BTC-USD']


def _extract(intervals: [TimeWindow]):
    range = TimeWindow(intervals[0].start, intervals[-1].end)
    trades = fetch_trades(range)
    return [features_from_subset(trades, w) for w in intervals]
