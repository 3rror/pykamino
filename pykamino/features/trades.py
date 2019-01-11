from pandas import DataFrame
from pandas import Series


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
        filter = self.timestamp.between(start_ts, end_ts)
        return self[filter]

    def buys(self):
        """Buys orders."""
        return self[self.side == "buy"]

    def sells(self):
        """Sells orders."""
        return self[self.side == "sell"]

    def mean_price(self):
        """Mean price."""
        return round(self.price.astype(float).mean(), 8)

    def std_price(self):
        """Standard deviation of prices."""
        return round(self.price.astype(float).std(), 8)

    def buy_count(self):
        """Number of buy orders."""
        return len(self.buys())

    def sell_count(self):
        """Number of sell orders."""
        return len(self.sells())

    def total_buy_volume(self):
        """Total amount buyed."""
        return round(self.buys().amount.astype(float).sum(), 8)

    def total_sell_volume(self):
        """Total amount selled."""
        return round(self.sells().amount.astype(float).sum(), 8)

    def price_movement(self):
        """Difference between first and last price."""
        if len(self) >= 1:
            index_first = self.id.idxmin()
            index_last = self.id.idxmax()
            first_trade = self.loc[index_first]
            last_trade = self.loc[index_last]
            return round(float(first_trade.price) - float(last_trade.price), 8)

    def features(self):
        return {
            "buy_count": self.buy_count(),
            "sell_count": self.sell_count(),
            "total_buy_volume": round(self.total_buy_volume(), 8),
            "total_sell_volume": round(self.total_sell_volume(), 8),
            "price_mean": self.mean_price(),
            "price_std": self.std_price(),
            "price_movement": self.price_movement(),
        }
