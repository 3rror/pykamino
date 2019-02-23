import itertools
import multiprocessing
from collections import namedtuple
from itertools import islice

import pandas

from pykamino.db import Trade, database
from pykamino.features.decorators import rounded

TimeWindow = namedtuple('TimeWindow', 'start, end')


def buys(trades):
    """Return trades of type 'buy' in the specified dataframe.

    Args:
        trades (pandas.DataFrame): dataframe of trades

    Returns:
        pandas.DataFrame: trades of type 'buy'

    """
    return trades[trades.side == "buy"]


def sells(trades):
    """Return trades of type 'sell' in the specified dataframe.

    Args:
        trades (pandas.DataFrame): dataframe of trades

    Returns:
        pandas.DataFrame: trades of type 'sell'

    """
    return trades[trades.side == "sell"]


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
            "Cannot calculate the mean price on an empty dataframe.")
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
            "Cannot calculate price standard deviation on an empty dataframe.")
    return trades.price.astype(float).std()


def buy_count(trades):
    """Return the number of 'buy' trades in the dataframe.

    Args:
        trades (pandas.DataFrame): dataframe of trades

    Returns:
        int: number of 'buy' trades

    """
    return len(buys(trades))


def sell_count(trades):
    """Return the number of 'sell' trades in the dataframe.

    Args:
        trades (pandas.DataFrame): dataframe of trades

    Returns:
        int: number of 'sell' trades

    """
    return len(sells(trades))


@rounded
def total_buy_volume(trades):
    """Return the total amount bought in the dataframe.

    Args:
        trades (pandas.DataFrame): dataframe of trades

    Returns:
        numpy.float64: total amount bought

    """
    return buys(trades).amount.sum()


@rounded
def total_sell_volume(trades):
    """Return the total amount sold in the dataframe.

    Args:
        trades (pandas.DataFrame): dataframe of trades

    Returns:
        numpy.float64: total amount sold

    """
    return sells(trades).amount.sum()


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
            "Cannot calculate price movement on an empty dataframe.")
    return oldest_trade(trades).price - latest_trade(trades).price


def latest_trade(trades):
    """Return the most recent trade in the dataframe.

    Args:
        trades (pandas.DataFrame): dataframe of trades

    Returns:
        [pandas.Series]: most recent trade

    """
    return trades.loc[trades.time.idxmax()]


def oldest_trade(trades):
    """Return the oldest trade in the dataframe.

    Args:
        trades (pandas.DataFrame): dataframe of trades

    Returns:
        [pandas.Series]: oldest trade

    """
    return trades.loc[trades.time.idxmin()]


def fetch_trades(start, end, product="BTC-USD"):
    """Return a dataframe of all the orders in the specified time window.

    Args:
        start (datetime.datetime): start of the time window
        end (datetime.datetime): end of the time window
        product (str, optional): Defaults to "BTC-USD". Currency to consider

    Returns:
        pandas.DataFrame: orders in the specified time window

    """
    trades = Trade.select().where(
        (Trade.product == product) & Trade.time.between(start, end)
    ).namedtuples()
    return pandas.DataFrame(trades)


def sliding_time_windows(start, end, freq, stride=100, chunksize=8):
    """Return a generator of sliding time windows.

    Args:
        start (datetime.datetime): start time
        end (datetime.datetime): end time
        freq (datetime.timedelta): resolution of each windows
        stride (int, optional):
            Defaults to 100. Offset of each time windows from the previous
            one, expressed as percentage of the resolution.

    Raises:
        ValueError:
            if stride is not a value greater than 0 and less or equal to 100
        ValueError:
            if frequency is greater than the period between start and end

    Returns:
        Generator[Tuple(datetime.datetime, datetime.datetime)]:
            a generator producing tuples like (window_start, window_end)

    """
    # A stride of 0 doesn't make sense because it would mean a 100% overlap
    # creating an infinite loop
    if not 0 < stride <= 100:
        raise ValueError(
            "Stride value must be greater than 0 and less or equal to 100.")

    if (end - start) < freq:
        raise ValueError(
            "Frequency must be less than the period between start and end")

    offset = freq * stride / 100

    buffer = []
    while start + freq <= end:
        if len(buffer) <= chunksize:
            buffer.append(TimeWindow(start, end=start + freq))
        else:
            yield buffer
            buffer.clear()
        start += offset

# def features_from_subset(trades, start, end):


def features_from_subset(trades, time_window):
    """
    TODO: Add doc
    """
    try:
        trades_slice = trades[trades.time.between(*time_window)]
        return {
            "buy_count": buy_count(trades_slice),
            "sell_count": sell_count(trades_slice),
            "total_buy_volume": total_buy_volume(trades_slice),
            "total_sell_volume": total_sell_volume(trades_slice),
            "price_mean": mean_price(trades_slice),
            "price_std": price_std(trades_slice),
            "price_movement": price_movement(trades_slice),
            "start_time": time_window.start,
            "end_time": time_window.end
        }
    except (ValueError, AttributeError):
        return {
            "buy_count": 0,
            "sell_count": 0,
            "total_buy_volume": 0,
            "total_sell_volume": 0,
            "price_mean": None,
            "price_std": None,
            "price_movement": None,
            "start_time": time_window.start,
            "end_time": time_window.end
        }


def batch_extract(start, end, res='10min', stride=10, products=("BTC-USD")):
    features = {}
    res = pandas.to_timedelta(res)
    with multiprocessing.Pool() as pool:
        for product in products:
            output = pool.imap(extract, sliding_time_windows(
                start, end, res, stride),  chunksize=200)
            features[product] = list(itertools.chain(output))
    return features['BTC-USD']


def extract(windows):
    start_first_window = windows[0].start
    end_last_windows = windows[-1].end
    trades = fetch_trades(start_first_window, end_last_windows)
    return [features_from_subset(trades, w) for w in windows]
