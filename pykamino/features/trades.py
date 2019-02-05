import multiprocessing
from datetime import datetime
from itertools import tee

import pandas
from pykamino.db import Trade


class TradesDataFrame(pandas.DataFrame):

    ROUND_DIGITS = 8

    def trades_between_instants(self, start_ts, end_ts):
        return self[self.time.between(start_ts, end_ts)]

    def buys(self):
        """Trades of type 'buy'."""
        return self[self.side == "buy"]

    def sells(self):
        """Trades of type 'sell'."""
        return self[self.side == "sell"]

    def price_mean(self):
        """Mean price."""
        return round(self.price.mean(), self.ROUND_DIGITS)

    def price_std(self):
        """Standard deviation of prices."""
        return round(self.price.astype(float).std(), self.ROUND_DIGITS)

    def buy_count(self):
        """Number of 'buy' trades."""
        return len(self.buys())

    def sell_count(self):
        """Number of 'sell' trades."""
        return len(self.sells())

    def total_buy_volume(self):
        """Total amount bought."""
        return round(self.buys().amount.sum(), self.ROUND_DIGITS)

    def total_sell_volume(self):
        """Total amount sold."""
        return round(self.sells().amount.sum(), self.ROUND_DIGITS)

    def price_movement(self):
        """Difference between the oldest and the most recent price."""
        if len(self) == 0:
            return None
        if len(self) == 1:
            return 0
        first_trade = self.loc[self.time.idxmin()]
        last_trade = self.loc[self.time.idxmax()]
        return round(first_trade.price - last_trade.price, self.ROUND_DIGITS)

    def compute_all(self):
        return {
            "buy_count": self.buy_count(),
            "sell_count": self.sell_count(),
            "total_buy_volume": self.total_buy_volume(),
            "total_sell_volume": self.total_sell_volume(),
            "price_mean": self.price_mean(),
            "price_std": self.price_std(),
            "price_movement": self.price_movement(),
        }


def _extract_instant_features(trades, instant, next_instant):
    trades_slice = trades.trades_between_instants(instant, next_instant)
    features = trades_slice.compute_all()
    features['time'] = instant
    return features


def trades_in_time_window(start_dt, end_dt, products):
    query = Trade.select().where(Trade.time.between(
        start_dt, end_dt) & Trade.product.in_(products))
    return TradesDataFrame(list(query.dicts()))


def extract(start_dt, end_dt, resolution='1min', products=['BTC-USD']):
    def pairwise(iterable):
        # https://docs.python.org/3.6/library/itertools.html#recipes
        a, b = tee(iterable)
        next(b, None)
        return zip(a, b)

    trades = trades_in_time_window(start_dt, end_dt, products)
    instants = instants = pandas.date_range(
        start=start_dt, end=end_dt, freq=resolution).tolist()
    with multiprocessing.Pool() as pool:
        params = [(trades, instant, next_instant)
                  for instant, next_instant in pairwise(instants)]
        features = pool.starmap(_extract_instant_features, params)
    return features
