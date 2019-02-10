import multiprocessing
from datetime import datetime
from itertools import tee

import pandas
from pykamino.db import Trade
from pykamino.features.decorators import rounded


def buys(df):
    """Trades of type 'buy'."""
    return df[df.side == "buy"]


def sells(df):
    """Trades of type 'sell'."""
    return df[df.side == "sell"]


@rounded
def price_mean(df):
    if len(df) == 0:
        return None
    return df.price.mean()


@rounded
def price_std(df):
    """Standard deviation of prices."""
    if len(df) == 0:
        return None
    return df.price.astype(float).std()


def buy_count(df):
    """Number of 'buy' trades."""
    return len(buys(df))


def sell_count(df):
    """Number of 'sell' trades."""
    return len(sells(df))


@rounded
def total_buy_volume(df):
    """Total amount bought."""
    return buys(df).amount.sum()


@rounded
def total_sell_volume(df):
    """Total amount sold."""
    return sells(df).amount.sum()


@rounded
def price_movement(df):
    """Difference between the oldest and the most recent price."""
    if len(df) == 0:
        return None
    if len(df) == 1:
        return 0
    first_trade = df.loc[df.time.idxmin()]
    last_trade = df.loc[df.time.idxmax()]
    return first_trade.price - last_trade.price


# TODO: make the collection automatic
def compute_all(df):
    return {
        "buy_count": buy_count(df),
        "sell_count": sell_count(df),
        "total_buy_volume": total_buy_volume(df),
        "total_sell_volume": total_sell_volume(df),
        "price_mean": price_mean(df),
        "price_std": price_std(df),
        "price_movement": price_movement(df),
    }


def select_trades(start_dt, end_dt, products):
    query = (Trade.select()
             .where(Trade.time.between(start_dt, end_dt) &
                    Trade.product.in_(products)))
    return pandas.DataFrame(list(query.dicts()))


def features_in_subset(df, instant, next_instant):
    trades_slice = df[df.time.between(instant, next_instant)]
    features = compute_all(trades_slice)
    features['time'] = instant
    return features


def extract(start_dt, end_dt, resolution='1min', products=['BTC-USD']):
    def pairwise(iterable):
        # https://docs.python.org/3.6/library/itertools.html#recipes
        a, b = tee(iterable)
        next(b, None)
        return zip(a, b)

    trades = select_trades(start_dt, end_dt, products)
    windows = pandas.date_range(start=start_dt, end=end_dt,
                                freq=resolution).tolist()
    with multiprocessing.Pool() as pool:
        params = [(trades, start, end) for start, end in pairwise(windows)]
        features = pool.starmap(features_in_subset, params)
    return features
