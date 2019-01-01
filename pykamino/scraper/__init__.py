from datetime import datetime
import itertools
import cbpro
from peewee import ProgrammingError, Case
from queue import Queue
from threading import Thread, Condition, Event

from pykamino.db import Order, OrderHistory as History, Trade, database
from pykamino.db.cbpro import Snapshot, msg_to_order_dict, msg_to_history_dict, msg_to_trade_dict


class Scraper():
    def __init__(self, products=['BTC-USD']):
        self._seqs = {prod: -1 for prod in products}
        self._receiver = Receiver(products=products)
        self._filter = Filter(sequences=self._seqs)
        self._storer = MessageStorer()

    @property
    def products(self):
        return self._seqs.keys()

    def save_snapshot(self, product):
        snap = Snapshot(product)
        seq = snap.download()
        snap.insert()
        return seq

    def start(self):
        # TODO: don't allow to start an already-started Scraper
        self._receiver.start()
        for p in self.products:
            # _seqs is a mutable object so it's passed by reference.
            # We don't need to pass it again to _filter
            self._seqs[p] = self.save_snapshot(p)
        self._filter.start()
        self._storer.start()

    def stop(self):
        self._receiver.stop()
        self._filter.stop()
        self._storer.stop()


### Threading stuff ###

class GracefulThread(Thread):
    def __init__(self):
        super().__init__()
        self._close_cond = Event()

    def run(self):
         while not self._close_cond.is_set():
             self.task()
    
    def task(self):
        raise NotImplementedError()

    def stop(self):
        self._close_cond.set()


msg_queue = Queue()
filtered_msgs = []
filt_msgs_lock = Condition()


class Receiver(cbpro.WebsocketClient):
    def __init__(self, **kwargs):
        super().__init__(channels=['full'], **kwargs)

    def on_open(self):
        pass

    def on_message(self, msg):
        msg_queue.put(msg)


class Filter(GracefulThread):
    def __init__(self, sequences=None):
        super().__init__()
        self._seqs = sequences

    def task(self):
        msg = msg_queue.get()
        msg_queue.task_done()
        try:
            if msg['sequence'] > self._seqs[msg['product_id']]:
                with filt_msgs_lock:
                    filtered_msgs.append(msg)
                    # TODO: make this magic number less magic
                    if len(filtered_msgs) >= 200:
                        filt_msgs_lock.notify_all()
        except KeyError:
            # The message received is probably the header message.
            # Safe to ignore.
            pass


class MessageStorer(GracefulThread):
    def task(self):
        with filt_msgs_lock:
            filt_msgs_lock.wait()
            trades = filter(lambda x: x['type']=='match', filtered_msgs)
            orders = filter(lambda x: x['type'] not in ['activate', 'match', 'received'], filtered_msgs)
            self.store_messages(orders, trades)
            filtered_msgs.clear()

    def store_messages(self, orders, trades):
        new_ord, hist, to_close = self.classify_orders(orders)
        trades = [msg_to_trade_dict(t) for t in trades]
        updates = Case(Order.id, [(o['id'], datetime.strptime(o['close_time'], '%Y-%m-%dT%H:%M:%S.%fZ')) for o in to_close])
        with database.atomic():
            if trades:
                Trade.insert_many(trades).execute()
            Order.insert_many(new_ord).execute()
            History.insert_many(hist).execute()
            with database.atomic():
                Order.update(close_time=updates).where(Order.id.in_([o['id'] for o in to_close])).execute()
    
    def classify_orders(self, orders):
        new_orders = []
        history = []
        to_close = []
        for o in orders:
            if o['type'] == 'done':
                to_close.append(msg_to_order_dict(o))
            elif o['type'] == 'change':
                history.append(msg_to_history_dict(o))
            else:
                new_orders.append(msg_to_order_dict(o))
                history.append(msg_to_history_dict(o))
        return new_orders, history, to_close
