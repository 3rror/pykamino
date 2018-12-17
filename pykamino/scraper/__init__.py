import itertools
import cbpro
import threading
from peewee import ProgrammingError

from pykamino.db import Order, OrderTimeline, Trade, database
from pykamino.db.cbpro import book_snapshot_to_orders, msg_to_order, msg_to_trade

# This module uses the Observer pattern


msg_lock = threading.RLock()


class Scraper():
    def __init__(self, products=['BTC-USD']):
        self.receiver = Receiver(products=products)
        self.query_client = cbpro.PublicClient()
        self.products = products

    def start(self):
        self.receiver.start()
        seq = self.save_book_snapshot()

        def is_newer(msg):
            try:
                msg['sequence'] > seq
            except KeyError:
                return False

        with msg_lock:
            self.receiver.messages = filter(is_newer, self.receiver.messages)
        self.receiver.subscribe(self.save_messages)


    def save_book_snapshot(self):
        orders = []
        timelines = []
        for prod in self.products:
            book_snap = self.query_client.get_product_order_book(prod, level=3)
            new_orders, new_timelines = zip(*book_snapshot_to_orders(book_snap, prod))
            orders.extend(new_orders)
            timelines.extend(new_timelines)
        # Don't insert already existing orders
        query = Order.select(Order.id).where(Order.id.in_([el.id for el in orders])).execute()
        filt = set((str(order.id) for order in query))
        orders = set(filter(lambda x: str(x.id) not in filt, orders))
        timelines = filter(lambda x: x.order in orders, timelines)
        with database.atomic():
            Order.bulk_create(orders)
            OrderTimeline.bulk_create(timelines)
        return book_snap['sequence']

    def save_messages(self, *args, **kwargs):
        with msg_lock:
            order_msgs, trade_msgs = self.classify_messages()
            orders, timelines = zip(*(msg_to_order(msg) for msg in order_msgs if msg['type'] != 'received'))
            orders = list(filter(lambda x: x is not None, orders))
            timelines = list(filter(lambda x: x is not None, timelines))

            query_filt = Order.select(Order.id).where(Order.id.in_([t.order_id for t in timelines])).execute()
            filt = set((str(r.id) for r in query_filt)).union(set((str(o.id) for o in orders)))
            #timelines = list(filter(lambda x: x.order_id in filt, timelines))
            print(timelines[0].order_id)
            import pdb
            pdb.set_trace()
            with database.atomic():
                #try:
                #    Trade.bulk_create((msg_to_trade(msg) for msg in trade_msgs))
                #except ProgrammingError:
                #    pass
                with database.atomic():
                    Order.bulk_create(orders)
                    #OrderTimeline.bulk_create(timelines)
            self.receiver.messages.clear()


    def classify_messages(self):
        """
        Split the list of messages in two iterators: orders and trades
        """
        orders, trades = itertools.tee(self.receiver.messages)

        def cond(msg): return msg['type'] == 'match'
        return itertools.filterfalse(cond, orders), filter(cond, trades)


class Receiver(cbpro.WebsocketClient):
    def __init__(self, buffer_size=100, **kwargs):
        super().__init__(channels=['full'], **kwargs)
        self.buffer_size = buffer_size
        self.observers = []

    def on_open(self):
        self._messages = []

    def on_message(self, msg):
        with msg_lock:
            self._messages.append(msg)
            msg_count = len(self._messages)
        if msg_count % self.buffer_size == 0:
            self.fire(msg_count=msg_count)

    def subscribe(self, callback):
        self.observers.append(callback)
        return callback

    def fire(self, *args, **kwargs):
        for callback in self.observers:
            callback(*args, **kwargs)
    
    @property
    def messages(self):
        return self._messages
    
    @messages.setter
    def messages(self, msgs):
        self._messages = msgs if isinstance(msgs, list) else list(msgs)
