import itertools

import cbpro
from peewee import ProgrammingError

from pykamino.db import Order, OrderTimeline, Trade, database
from pykamino.db.cbpro import book_snapshot_to_orders

# This module uses the Observer pattern


class Scraper():
    def __init__(self, products=['BTC-USD']):
        self.receiver = Receiver(products=products)
        self.query_client = cbpro.PublicClient()
        self.products = products

    def start(self):
        self.receiver.start()
        self.save_book_snapshot()

    def save_book_snapshot(self):
        # TODO: support more than one product at time
        book_snap = self.query_client.get_product_order_book(self.products[0], level=3)
        orders, timelines = book_snapshot_to_orders(book_snap, self.products[0])
        # Don't insert already existing orders
        query = Order.select(Order.id).where(Order.id.in_([el.id for el in orders])).execute()
        filt = set((str(order.id) for order in query))
        orders = set(filter(lambda x: str(x.id) not in filt, orders))
        timelines = filter(lambda x: x.order in orders, timelines)
        with database.atomic():
            Order.bulk_create(orders)
            OrderTimeline.bulk_create(timelines)

    def classify_messages(self, msg_list):
        """
        Split the list of messages in two iterators: orders and trades
        """
        orders, trades = itertools.tee(msg_list)

        def cond(msg): return msg['type'] == 'match'
        return itertools.filterfalse(cond, orders), filter(cond, trades)


class Receiver(cbpro.WebsocketClient):
    def __init__(self, buffer_size=100, **kwargs):
        super().__init__(channels=['full'], **kwargs)
        self.buffer_size = buffer_size
        self.observers = []

    def on_open(self):
        self.messages = list()

    def on_message(self, msg):
        self.messages.append(msg)
        msg_count = len(self.messages)
        if msg_count % self.buffer_size == 0:
            self.fire(msg_count=msg_count)

    def subscribe(self, callback):
        self.observers.append(callback)
        return callback

    def fire(self, *args, **kwargs):
        for callback in self.observers:
            callback(*args, **kwargs)
