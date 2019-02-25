import unittest
from abc import abstractmethod
from datetime import datetime
from datetime import timedelta as delta
from decimal import Decimal

from peewee import SqliteDatabase
from pykamino.db import Dbms, Order, OrderHistory, Trade, db_factory
from pykamino.features import orders, trades
from pykamino.features import TimeWindow


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
    UPDATE_DT = START_DT + delta(hours=3)
    CLOSE_DT = START_DT + delta(hours=5)
    N_ORDERS = 20

    @staticmethod
    def uuid_builder(i):
        num = '{:032x}'.format(i)
        return '{}-{}-{}-{}-{}'.format(num[0:8], num[8:12], num[12:16], num[16:20], num[20:])

    @property
    def models(self):
        return [Order, OrderHistory]

    def prepare_dataframes(self):
        start = self.CLOSE_DT + delta(minutes=10)
        end = self.CLOSE_DT + delta(minutes=20)
        self.orders = orders.select_orders(start, end, ['BTC-USD'])
        self.order_book = orders.order_book_from_cache(
            self.orders,
            self.UPDATE_DT + delta(minutes=10))

    def populate_tables(self):
        orders = []
        hist = []
        for i in range(0, self.N_ORDERS):
            orders.append(
                (self.uuid_builder(i),
                 'bid' if i % 2 == 0 else 'ask',
                 'BTC-USD',
                 1500 + 500 * i,
                 self.CLOSE_DT + delta(minutes=i) if i % 3 == 0 else None))
            hist.append(
                (i+1,
                 Decimal('0.1') * (i+1),
                 self.START_DT + delta(minutes=i),
                 self.uuid_builder(i)))
        Order.insert_many(orders, fields=Order._meta.fields).execute()
        OrderHistory.insert_many(hist, OrderHistory._meta.fields).execute()
        OrderHistory.insert(
            {'id': 100,
             'amount': 100,
             'time': self.UPDATE_DT,
             'order_id': self.uuid_builder(9)}).execute()
        OrderHistory.insert(
            {'id': 200,
             'amount': 100,
             'time': self.UPDATE_DT,
             'order_id': self.uuid_builder(11)}).execute()

    def setUp(self):
        super().setUp()
        self.prepare_dataframes()

    def test_database_query(self):
        self.assertEqual(len(self.orders), 17)

    def test_book_from_cache(self):
        with self.subTest():
            self.assertEqual(len(self.order_book), 16)
            df = self.order_book
            row = df[df.id.astype(str) == self.uuid_builder(11)]
            self.assertEqual(row.amount.iloc[0], 100)

    def test_best_ask_price(self):
        self.assertEqual(
            orders.best_ask_price(self.order_book), 2000)

    def test_best_bid_price(self):
        self.assertEqual(
            orders.best_bid_price(self.order_book), 10500)

    def test_best_ask_amount(self):
        Order.insert(
            {'id': self.uuid_builder(900),
             'side': 'ask',
             'product': 'BTC-USD',
             'price': 2000,
             'close_time': None}).execute()
        OrderHistory.insert(
            {'id': 900,
             'amount': 1,
             'time': self.START_DT,
             'order_id': self.uuid_builder(900)}).execute()
        self.prepare_dataframes()
        self.assertEqual(
            orders.best_ask_amount(self.order_book), Decimal('1.2'))

    def test_best_bid_amount(self):
        Order.insert(
            {'id': self.uuid_builder(900),
             'side': 'bid',
             'product': 'BTC-USD',
             'price': 10500,
             'close_time': None}).execute()
        OrderHistory.insert(
            {'id': 900,
             'amount': 1,
             'time': self.START_DT,
             'order_id': self.uuid_builder(900)}).execute()
        self.prepare_dataframes()
        self.assertEqual(
            orders.best_bid_amount(self.order_book), Decimal('2.9'))

    def test_mid_market_price(self):
        self.assertEqual(orders.mid_market_price(self.order_book), 6250)

    def test_spread(self):
        self.assertEqual(orders.spread(self.order_book), 8500)

    def test_ask_depth(self):
        self.assertEqual(orders.ask_depth(self.order_book), 8)

    def test_bid_depth(self):
        self.assertEqual(orders.bid_depth(self.order_book), 8)

    def test_ask_depth_chart(self):
        chart = orders.ask_depth_chart(self.order_book)
        with self.subTest():
            self.assertEqual(chart.amount.iloc[0], Decimal('0.2'))
            self.assertEqual(chart.amount.iloc[-1], Decimal('108.4'))

    def test_bid_depth_chart(self):
        chart = orders.bid_depth_chart(self.order_book)
        first_row = chart.iloc[0]
        last_row = chart.iloc[-1]
        with self.subTest():
            self.assertEqual(first_row.amount, Decimal('9.2'))
            self.assertEqual(first_row.price, Decimal(2500))
            self.assertEqual(last_row.amount, Decimal('1.9'))
            self.assertEqual(last_row.price, Decimal(10500))

    def test_ask_volume_weighted(self):
        self.assertAlmostEqual(orders.ask_volume_weighted(
            self.order_book), Decimal('0.13466247'), delta=1e-8)

    def test_bid_volume_weighted(self):
        self.assertAlmostEqual(orders.bid_volume_weighted(
            self.order_book), Decimal('-0.00561498'), delta=1e-8)

    def test_ask_volume(self):
        self.assertEqual(orders.ask_volume(self.order_book), Decimal('108.4'))

    def test_bid_volume(self):
        self.assertEqual(orders.bid_volume(self.order_book), Decimal('9.2'))


class TradeFeatures(BaseTestCase):
    START_DT = datetime(2010, 1, 30, 11, 00)

    @property
    def models(self):
        return [Trade]

    def populate_tables(self):
        data = []
        for i in range(0, 20):
            data.append(
                (i + 1,
                 'sell' if i < 10 else 'buy',
                 Decimal('0.1') * (i+1),
                 'BTC-USD',
                 1500 + 500 * i,
                 self.START_DT + delta(minutes=10 * i)))
        Trade.insert_many(data, fields=Trade._meta.fields).execute()

    def setUp(self):
        super().setUp()
        interval = TimeWindow(self.START_DT, datetime.max)
        self.dataframe = trades.fetch_trades(interval, product='BTC-USD')

    def test_mean_price(self):
        self.assertEqual(trades.mean_price(self.dataframe), 6250)

    def test_std_price(self):
        self.assertAlmostEqual(trades.price_std(self.dataframe),
                               2958.03989154, delta=1e-8)

    def test_buy_count(self):
        self.assertEqual(trades.buy_count(self.dataframe), 10)

    def test_sell_count(self):
        self.assertEqual(trades.sell_count(self.dataframe), 10)

    def test_total_buy_volume(self):
        self.assertEqual(trades.total_buy_volume(self.dataframe), 15.5)

    def test_total_sell_volume(self):
        self.assertEqual(trades.total_sell_volume(self.dataframe), 5.5)

    def test_price_movement(self):
        self.assertEqual(trades.price_movement(self.dataframe), -9500)

    def test_features_from_subset(self):
        # Pandas can be very unintuitive. Let's test if we can get
        # features from a subset of the dataframe as well...
        interval = TimeWindow(self.START_DT, self.START_DT + delta(minutes=46))
        subset = trades.features_from_subset(self.dataframe, interval)
        self.assertEqual(subset['price_mean'], 2500)

    # TODO: test CSV generation, not only calculations


if __name__ == '__main__':
    unittest.main()
