import unittest
from abc import abstractmethod
from datetime import datetime
from datetime import timedelta as delta
from decimal import Decimal

from peewee import SqliteDatabase

from pykamino.db import OrderState, Trade
from pykamino.features import TimeWindow, orders, trades


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
        return [OrderState]

    def prepare_dataframes(self):
        start = self.CLOSE_DT + delta(minutes=self.N_ORDERS/2)
        end = self.CLOSE_DT + delta(minutes=self.N_ORDERS)
        self.order_states = orders.fetch_states(
            TimeWindow(start, end), 'BTC-USD')

    def populate_tables(self):
        order_states = []
        for i in range(self.N_ORDERS):
            # Add orders states with the following rules:
            # • Bid and ask orders are alternate
            # • If i is divisible by 3, then it's closed
            order_states.append({
                'order_id': self.uuid_builder(i),
                'product': 'BTC-USD',
                'side': 'bid' if i % 2 == 0 else 'ask',
                'price': 1500 + 500 * i,
                'amount': Decimal('0.1') * (i+1),
                'starting_at': self.START_DT + delta(minutes=i),
                # We want 1 order closed, then 2 left open, and so on
                'ending_at': self.CLOSE_DT + delta(minutes=i) if i % 3 == 0 else None
            })
        order_states[9]['ending_at'] = self.UPDATE_DT
        order_states.append({
            **order_states[9],
            'amount': 100,
            'starting_at': self.UPDATE_DT,
            'ending_at': self.CLOSE_DT + delta(minutes=9)
        })

        order_states[11]['ending_at'] = self.UPDATE_DT
        order_states.append({
            **order_states[11],
            'amount': 100,
            'starting_at': self.UPDATE_DT,
            'ending_at': None
        })

        OrderState.insert_many(order_states).execute()

    def setUp(self):
        super().setUp()
        self.prepare_dataframes()
        self.filtered_states = orders.get_open_orders(
            self.order_states, self.UPDATE_DT + delta(minutes=10))

    def test_database_query(self):
        self.assertEqual(len(self.order_states), 16)

    def test_best_ask_price(self):
        self.assertEqual(orders.best_ask_price(self.filtered_states), 2000)

    def test_best_bid_price(self):
        self.assertEqual(orders.best_bid_price(self.filtered_states), 10500)

    def test_best_ask_amount(self):
        OrderState.insert({
            'order_id': self.uuid_builder(900),
            'side': 'ask',
            'product': 'BTC-USD',
            'price': 2000,
            'amount': 1,
            'starting_at': self.START_DT
        }).execute()
        self.prepare_dataframes()
        filtered_states = orders.get_open_orders(
            self.order_states, self.UPDATE_DT + delta(minutes=10))
        self.assertEqual(orders.best_ask_amount(filtered_states), 1.2)

    def test_best_bid_amount(self):
        OrderState.insert({
            'order_id': self.uuid_builder(900),
            'side': 'bid',
            'product': 'BTC-USD',
            'price': 10500,
            'amount': 1,
            'starting_at': self.START_DT
        }).execute()
        self.prepare_dataframes()
        filtered_states = orders.get_open_orders(
            self.order_states, self.UPDATE_DT + delta(minutes=10))
        self.assertEqual(orders.best_bid_amount(filtered_states), 2.9)

    def test_mid_market_price(self):
        self.assertEqual(orders.mid_market_price(self.filtered_states), 6250)

    def test_bid_ask_spread(self):
        self.assertEqual(orders.bid_ask_spread(self.filtered_states), 8500)

    def test_ask_depth(self):
        self.assertEqual(orders.ask_depth(self.filtered_states), 8)

    def test_bid_depth(self):
        self.assertEqual(orders.bid_depth(self.filtered_states), 8)

    def test_ask_volume_weighted(self):
        self.assertAlmostEqual(
            orders.ask_volume_weighted(self.filtered_states), 0.13466247, delta=1e-8)

    def test_bid_volume_weighted(self):
        self.assertAlmostEqual(
            orders.bid_volume_weighted(self.filtered_states), -0.00561498, delta=1e-8)

    def test_ask_volume(self):
        self.assertEqual(orders.ask_volume(self.filtered_states), 108.4)

    def test_bid_volume(self):
        self.assertEqual(orders.bid_volume(self.filtered_states), 9.2)


class TradeFeatures(BaseTestCase):
    START_DT = datetime(2010, 1, 30, 11, 00)
    N_TRADES = 20

    @property
    def models(self):
        return [Trade]

    def populate_tables(self):
        data = []
        for i in range(self.N_TRADES):
            data.append({
                'id': i + 1,
                'side': 'sell' if i < 10 else 'buy',
                'amount': 0.1 * (i+1),
                'product': 'BTC-USD',
                'price': 1500 + 500 * i,
                'time': self.START_DT + delta(minutes=10 * i)})
        Trade.insert_many(data).execute()

    def setUp(self):
        super().setUp()
        interval = TimeWindow(self.START_DT, datetime.max)
        self.dataframe = trades.fetch_trades(interval, product='BTC-USD')

    def test_mean_price(self):
        self.assertEqual(trades.price_mean(self.dataframe), 6250)

    def test_std_price(self):
        self.assertAlmostEqual(trades.price_std(self.dataframe),
                               2883.14064867, delta=1e-8)

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

    def test_compute_all_features(self):
        # Pandas can be very unintuitive. Let's test if we can get
        # features from a subset of the dataframe as well...
        interval = TimeWindow(self.START_DT, self.START_DT + delta(minutes=46))
        subset = trades.compute_all_features(self.dataframe, interval)
        self.assertEqual(subset['price_mean'], 2500)

    # TODO: test CSV generation, not only calculations


if __name__ == '__main__':
    unittest.main()
