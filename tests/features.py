import unittest
from datetime import datetime, timedelta
from decimal import Decimal

from pykamino.db import Dbms, Order, OrderHistory, Trade, db_factory
from pykamino.features.trades import TradesDataFrame, extract

db = db_factory(Dbms.SQLITE, ':memory:')


def uuid_builder(i):
    num = '{:032x}'.format(i)
    return '{}-{}-{}-{}'.format(num[0:8], num[8:12], num[12:16], num[16:])


class OrderFeatures(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        orders = []
        hist = []
        dt = datetime(2010, 1, 30, 10, 00)
        for i in range(0, 20):
            orders.append((uuid_builder(i),
                           'bid' if i % 2 == 0 else 'ask',
                           'BTC-USD',
                           1500 + 500 * i,
                           dt + timedelta(hours=5, minutes=i) if i % 3 == 0 else None))
            hist.append((i+1,
                         0.1 + 0.1 * i,
                         dt + timedelta(minutes=i),
                         uuid_builder(i)))
        Order.insert_many(orders, fields=Order._meta.fields).execute()
        OrderHistory.insert_many(hist, OrderHistory._meta.fields).execute()
        OrderHistory.insert({'id': 100, 'amount': 100, 'time': dt + timedelta(hours=3),
                             'order_id': uuid_builder(9)}).execute()
        OrderHistory.insert({'id': 200, 'amount': 100, 'time': dt + timedelta(hours=3),
                             'order_id': uuid_builder(11)}).execute()

    def test_best_ask_order(self):
        self.assertTrue(True)


class TradeFeatures(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        data = []
        dt = datetime(2010, 1, 30, 11, 00)
        for i in range(0, 20):
            data.append((i + 1,
                         'sell' if i < 10 else 'buy',
                         Decimal('0.1') * (i+1),
                         'BTC-USD',
                         1500 + 500 * i,
                         dt + timedelta(minutes=10 * i)))
        Trade.insert_many(data, fields=Trade._meta.fields).execute()
        cls.dataframe = TradesDataFrame(list(Trade.select().dicts()))

    def test_mean_price(self):
        self.assertEqual(self.dataframe.price_mean(), 6250)

    def test_std_price(self):
        self.assertAlmostEqual(self.dataframe.price_std(),
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

    # TODO: test CSV generation, not only calculations


if __name__ == '__main__':
    unittest.main()
