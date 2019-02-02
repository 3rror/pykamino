import unittest
from datetime import datetime, timedelta

from pykamino.db import Dbms, Trade, db_factory
from pykamino.features.trades import TradesDataFrame

db = db_factory(Dbms.SQLITE, ':memory:')


class OrderFeatures(unittest.TestCase):
    pass


class TradeFeatures(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        data = []
        dt = datetime(2010, 1, 30, 11, 00)
        for i in range(0, 20):
            data.append((i + 1,
                         'sell' if i < 10 else 'buy',
                         0.1 + 0.1 * i,
                         'BTC-USD',
                         1500 + 500 * i,
                         dt + timedelta(minutes=10 * i)))
        Trade.insert_many(data, fields=Trade._meta.fields).execute()
        cls.dataframe = TradesDataFrame(list(Trade.select().dicts()))

    def test_mean_price(self):
        self.assertEqual(self.dataframe.mean_price(), 6250)

    def test_std_price(self):
        self.assertAlmostEqual(self.dataframe.std_price(),
                               2958.03989154, delta=1e-8)

    def test_buy_count(self):
        self.assertEqual(self.dataframe.buy_count(), 10)

    def test_sell_count(self):
        self.assertEqual(self.dataframe.sell_count(), 10)

    def test_total_buy_volume(self):
        self.assertEqual(self.dataframe.total_buy_volume(), 15.5)

    def test_total_sell_volume(self):
        self.assertEqual(self.dataframe.total_sell_volume(), 5.5)

    def test_price_movement(self):
        self.assertEqual(self.dataframe.price_movement(), -9500)


if __name__ == '__main__':
    unittest.main()
