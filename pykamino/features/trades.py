import multiprocessing
from itertools import tee

import pandas
from pandas import DataFrame, Series

from pykamino.db import Trade


class TradesSeries(Series):
    @property
    def _constructor(self):
        return TradesSeries

    @property
    def _constructor_expanddim(self):
        return TradesDataFrame


class TradesDataFrame(DataFrame):
    @property
    def _constructor(self):
        return TradesDataFrame

    @property
    def _constructor_sliced(self):
        return TradesSeries

    def trades_between_instants(self, start_ts, end_ts):
        return self[self.time.between(start_ts, end_ts)]

    def buys(self):
        """Trades of type 'buy'."""
        return self[self.side == "buy"]

    def sells(self):
        """Trades of type 'sell'."""
        return self[self.side == "sell"]

    def mean_price(self):
        """Mean price."""
        return round(self.price.mean(), 8)

    def std_price(self):
        """Standard deviation of prices."""
        return round(self.price.astype(float).std(), 8)

    def buy_count(self):
        """Number of 'buy' trades."""
        return len(self.buys())

    def sell_count(self):
        """Number of 'sell' trades."""
        return len(self.sells())

    def total_buy_volume(self):
        """Total amount bought."""
        return round(self.buys().amount.sum(), 8)

    def total_sell_volume(self):
        """Total amount sold."""
        return round(self.sells().amount.sum(), 8)

    def price_movement(self):
        """Difference between the oldest and the most recent price."""
        if len(self) >= 1:
            index_first = self.id.idxmin()
            index_last = self.id.idxmax()
            first_trade = self.loc[index_first]
            last_trade = self.loc[index_last]
            return round(first_trade.price - last_trade.price, 8)

    def compute_all(self):
        return {
            "buy_count": self.buy_count(),
            "sell_count": self.sell_count(),
            "total_buy_volume": round(self.total_buy_volume(), 8),
            "total_sell_volume": round(self.total_sell_volume(), 8),
            "price_mean": self.mean_price(),
            "price_std": self.std_price(),
            "price_movement": self.price_movement(),
        }


def _extract_instant_features(trades, instant, next_instant):
    trades_slice = trades.trades_between_instants(instant, next_instant)
    features = trades_slice.compute_all()
    features['time'] = instant
    return features


def _pairwise(iterable):
    # https://docs.python.org/3.6/library/itertools.html#recipes
    a, b = tee(iterable)
    next(b, None)
    return zip(a, b)


def trades_in_time_window(start_dt, end_dt, products):
    query = Trade.select().where(Trade.time.between(
        start_dt, end_dt), Trade.product.in_(products))
    return TradesDataFrame(list(query.dicts()))


def extract(start_dt, end_dt, resolution='1min', products=['BTC-USD']):
    trades = trades_in_time_window(start_dt, end_dt, products)
    instants = instants = pandas.date_range(
        start=start_dt, end=end_dt, freq=resolution).tolist()
    with multiprocessing.Pool() as pool:
        params = [(trades, instant, next_instant)
                  for instant, next_instant in _pairwise(instants)]
        features = pool.starmap(_extract_instant_features, params)
    return features
