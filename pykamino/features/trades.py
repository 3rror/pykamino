import multiprocessing
import pandas
from pykamino.db import Trade
from pykamino.features.decorators import rounded


def buys(trades):
    """Trades of type 'buy'."""
    return trades[trades.side == "buy"]


def sells(trades):
    """Trades of type 'sell'."""
    return trades[trades.side == "sell"]


@rounded
def price_mean(trades):
    if trades.empty:
        return None
    return trades.price.mean()


@rounded
def price_std(trades):
    """Standard deviation of prices."""
    if trades.empty:
        return None
    return trades.price.astype(float).std()


def buy_count(trades):
    """Number of 'buy' trades."""
    return len(buys(trades))


def sell_count(trades):
    """Number of 'sell' trades."""
    return len(sells(trades))


@rounded
def total_buy_volume(trades):
    """Total amount bought."""
    return buys(trades).amount.sum()


@rounded
def total_sell_volume(trades):
    """Total amount sold."""
    return sells(trades).amount.sum()


@rounded
def price_movement(trades):
    """Difference between the oldest and the most recent price."""
    if trades.empty:
        return None
    # Avoid results like 0E0
    if len(trades) == 1:
        return 0
    first_trade = trades.loc[trades.time.idxmin()]
    last_trade = trades.loc[trades.time.idxmax()]
    return first_trade.price - last_trade.price


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
