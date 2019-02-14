import multiprocessing

import pandas

from pykamino.db import Trade
from pykamino.features.decorators import rounded


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
    """Return the difference between the oldest and the most recent trade price
    in the dataframe.

    Args:
        trades (pandas.DataFrame): dataframe of trades

    Raises:
        ValueError: raised when trades dataframe is empty

    Returns:
        decimal.Decimal: price movement

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


def compute_all(trades):
    return {
        "buy_count": buy_count(trades),
        "sell_count": sell_count(trades),
        "total_buy_volume": total_buy_volume(trades),
        "total_sell_volume": total_sell_volume(trades),
        "price_mean": price_mean(trades),
        "price_std": price_std(trades),
        "price_movement": price_movement(trades),
    }


def select_trades(start, end, products):
    """
    Return from the database all the order in the specified time window.
    """
    trades = Trade.select().where(
        Trade.time.between(start, end) & Trade.product.in_(products)
    )
    return pandas.DataFrame(list(trades.dicts()))


def time_windows(start, end, freq, stride=10):
    """
    Create a generator of time windows.
    """
    if not 0 < stride <= 100:
        raise ValueError(
            "Stride value must be greater than 0 and less or equal to 100.")
    offset = freq * stride / 100
    while start + freq <= end:
        yield start, start + freq
        start += offset


def features_from_subset(trades, start, end):
    """
    TODO: Add doc
    """
    trades_slice = trades[trades.time.between(start, end)]
    features = compute_all(trades_slice)
    features["start_time"] = start
    features["end_time"] = end

    return features


def extract(start, end, res='10min', stride=10, products=["BTC-USD"]):
    """
    TODO: Add doc
    """
    res = pandas.to_timedelta(res)

    # Pre-download all orders in the time window. In-memory filtering is
    # faster than sending multiple queries to the database.
    trades = select_trades(start, end, products)

    with multiprocessing.Pool() as pool:
        data = ((trades, *window) for window in time_windows(start, end, res, stride))
        return pool.starmap(features_from_subset, data)
