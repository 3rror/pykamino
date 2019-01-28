from datetime import datetime
from queue import Queue
from threading import Condition, Event, Thread

from cbpro import WebsocketClient
from peewee import Case

from pykamino.db import Order
from pykamino.db import OrderHistory as History
from pykamino.db import Trade, database
from pykamino.scraper.snapshot import Snapshot


class Client():
    def __init__(self, buffer_len, products=['BTC-USD']):
        self._seqs = {prod: -1 for prod in products}
        self._receiver = Receiver(products=products)
        self._filter = Filter(buffer_len, sequences=self._seqs)
        self._storer = MessageStorer()
        self._is_running = False

    @property
    def products(self):
        return self._seqs.keys()

    @property
    def buffer_length(self):
        return self._filter.buffer_len

    @buffer_length.setter
    def buffer_length(self, value):
        self._filter.buffer_len = value

    def save_snapshot(self, product):
        snap = Snapshot(product)
        seq = snap.download()
        snap.insert()
        return seq

    @property
    def is_running(self):
        return self._is_running

    def start(self):
        if not self._is_running:
            self._is_running = True
            self._receiver.start()
            for p in self.products:
                # _seqs is a mutable object so it's been passed by reference.
                # We don't need to pass it again to _filter
                self._seqs[p] = self.save_snapshot(p)
            self._filter.start()
            self._storer.start()
        else:
            raise RuntimeError('The scraper is already running.')

    def stop(self):
        self._receiver.stop()
        self._filter.stop()
        self._storer.stop()
        self._is_running = False


### Threading stuff ###

class GracefulThread(Thread):
    """
    A thread that can be graceously stopped by calling `stop()`.

    Override `task()` to define its activity. Do note that `task()`
    will be called in a loop.
    """

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


class Receiver(WebsocketClient):
    def __init__(self, **kwargs):
        super().__init__(channels=['full'], **kwargs)

    def on_open(self):
        pass

    def on_message(self, msg):
        msg_queue.put(msg)


class Filter(GracefulThread):
    def __init__(self, buffer_len, sequences=None):
        super().__init__()
        self.sequences = sequences
        self.buffer_len = buffer_len

    def task(self):
        msg = msg_queue.get()
        msg_queue.task_done()
        try:
            if msg['sequence'] > self.sequences[msg['product_id']]:
                self.append_filtered(msg)
        except KeyError:
            pass

    def append_filtered(self, msg):
        with filt_msgs_lock:
            filtered_msgs.append(msg)
            if len(filtered_msgs) >= self.buffer_len:
                filt_msgs_lock.notify_all()


class MessageStorer(GracefulThread):
    def task(self):
        with filt_msgs_lock:
            filt_msgs_lock.wait()
            trades = (m for m in filtered_msgs if m['type'] == 'match')
            orders = (m for m in filtered_msgs if m['type'] not in
                      ['activate', 'match', 'received'])
            self.store_messages(orders, trades)
            filtered_msgs.clear()

    def store_messages(self, orders, trades):
        new_ord, hist, to_close = self.classify_orders(orders)
        trades = [msg_to_trade_dict(t) for t in trades]
        id_then_time = Case(Order.id, [(o['id'], datetime.strptime(
            o['close_time'], Order.close_time.formats[0])) for o in to_close])
        with database.atomic():
            if trades:
                Trade.insert_many(trades).execute()
            Order.insert_many(new_ord).execute()
            History.insert_many(hist).execute()
            with database.atomic():
                Order.update(close_time=id_then_time).where(
                    Order.id.in_([o['id'] for o in to_close])).execute()

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


def msg_to_order_dict(msg):
    return {
        'id': msg['order_id'],
        'side': msg['side'],
        'product': msg['product_id'],
        'price': msg.get('price'),
        'close_time': msg['time'] if msg['type'] == 'done' else None
    }


def msg_to_history_dict(msg):
    return {
        'size': msg['remaining_size'],
        'time': msg['time'],
        'order': msg['order_id']
    }


def msg_to_trade_dict(msg):
    return {
        'side': msg['side'],
        'size': msg['size'],
        'product': msg['product_id'],
        'price': msg['price'],
        'time': msg['time']
    }
