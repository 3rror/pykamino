from pykamino.features.trades import *
import unittest
from abc import abstractmethod
from datetime import datetime, timedelta
from decimal import Decimal

from peewee import SqliteDatabase
from pykamino.db import Dbms, Order, OrderHistory, Trade, db_factory


def uuid_builder(i):
    num = '{:032x}'.format(i)
    return '{}-{}-{}-{}'.format(num[0:8], num[8:12], num[12:16], num[16:])


class BaseTestCase(unittest.TestCase):
    def setUp(self):
        self.db = SqliteDatabase(':memory:')
        self.db.bind(self.models)
        self.db.connect()
        self.db.create_tables(self.models)
        self.populate_tables()

    def tearDown(self):
        self.db.drop_tables(self.models)
        self.db.close()

    @property
    @abstractmethod
    def models(self):
        pass

    @abstractmethod
    def populate_tables(self):
        pass


class OrderFeatures(BaseTestCase):

    START_DT = datetime(2010, 1, 30, 11, 00)

    @property
    def models(self):
        return [Order, OrderHistory]

    def populate_tables(self):
        orders = []
        hist = []
        for i in range(0, 20):
            orders.append((uuid_builder(i),
                           'bid' if i % 2 == 0 else 'ask',
                           'BTC-USD',
                           1500 + 500 * i,
                           self.START_DT + timedelta(hours=5, minutes=i) if i % 3 == 0 else None))
            hist.append((i+1,
                         Decimal('0.1') * (i+1),
                         self.START_DT + timedelta(minutes=i),
                         uuid_builder(i)))
        Order.insert_many(orders, fields=Order._meta.fields).execute()
        OrderHistory.insert_many(hist, OrderHistory._meta.fields).execute()
        OrderHistory.insert({'id': 100, 'amount': 100, 'time': self.START_DT + timedelta(hours=3),
                             'order_id': uuid_builder(9)}).execute()
        OrderHistory.insert({'id': 200, 'amount': 100, 'time': self.START_DT + timedelta(hours=3),
                             'order_id': uuid_builder(11)}).execute()

    def setUp(self):
        super().setUp()
        # self.dataframe =

    def test_best_ask_order(self):
        self.assertTrue(True)


class TradeFeatures(BaseTestCase):
    START_DT = datetime(2010, 1, 30, 11, 00)

    @property
    def models(self):
        return [Trade]

    def populate_tables(self):
        data = []
        for i in range(0, 20):
            data.append((i + 1,
                         'sell' if i < 10 else 'buy',
                         Decimal('0.1') * (i+1),
                         'BTC-USD',
                         1500 + 500 * i,
                         self.START_DT + timedelta(minutes=10 * i)))
        Trade.insert_many(data, fields=Trade._meta.fields).execute()

    def setUp(self):
        super().setUp()
        self.dataframe = select_trades(
            self.START_DT, self.START_DT + timedelta(weeks=1), products=['BTC-USD'])

    def test_mean_price(self):
        self.assertEqual(price_mean(self.dataframe), 6250)

    def test_std_price(self):
        self.assertAlmostEqual(price_std(self.dataframe),
                               2958.03989154, delta=1e-8)

    def test_buy_count(self):
        self.assertEqual(buy_count(self.dataframe), 10)

    def test_sell_count(self):
        self.assertEqual(sell_count(self.dataframe), 10)

    def test_total_buy_volume(self):
        self.assertEqual(total_buy_volume(self.dataframe), 15.5)

    def test_total_sell_volume(self):
        self.assertEqual(total_sell_volume(self.dataframe), 5.5)

    def test_price_movement(self):
        self.assertEqual(price_movement(self.dataframe), -9500)

    def test_features_from_subset(self):
        # Pandas can be very unintuitive. Let's test if we can get
        # features from a subset of the dataframe as well...
        subset = features_from_subset(self.dataframe,
                                      self.START_DT, self.START_DT + timedelta(minutes=46))
        self.assertEqual(subset['price_mean'], 2500)

    # TODO: test CSV generation, not only calculations


if __name__ == '__main__':
    unittest.main()
