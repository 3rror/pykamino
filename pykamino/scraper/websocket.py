from queue import Queue
from threading import Condition, Event, Thread

from cbpro import WebsocketClient

from peewee import Case
from pykamino.db import OrderState
from pykamino.db import Trade
from pykamino.scraper.snapshot import Snapshot

from datetime import datetime


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

    Override `task()` to define its activity. Do note that `task()` will be
    called in a loop.
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
            self.store_messages()
            filtered_msgs.clear()

    def _split_orders_trades(self):
        order_msgs = []
        trade_msgs = []

        for msg in filtered_msgs:
            if msg['type'] == 'match':
                trade_msgs.append(msg)
                continue
            if msg['type'] not in ('activate', 'received'):
                order_msgs.append(msg)
        return order_msgs, trade_msgs

    def _split_orders(self, order_msgs):
        orders_to_close = []
        new_orders = []
        for order_msg in order_msgs:
            if order_msg['type'] == 'done':
                orders_to_close.append(msg_to_order_dict(order_msg))
            elif order_msg['type'] == 'change':
                base_order = msg_to_order_dict(order_msg)
                orders_to_close.append({
                    **base_order,
                    'ending_at': order_msg['time']})

                new_orders.append({
                    **base_order,
                    'starting_at': order_msg['time'],
                    'amount': order_msg['new_size']})
            elif order_msg['type'] == 'open':
                new_orders.append(msg_to_order_dict(order_msg))
        return new_orders, orders_to_close

    def store_messages(self):
        order_msgs, trade_msgs = self._split_orders_trades()
        new_states, states_to_close = self._split_orders(order_msgs)
        if trade_msgs:
            (Trade
                .insert_many([msg_to_trade_dict(msg) for msg in trade_msgs])
                .execute())
        # Insert new states
        if new_states:
            (OrderState
                .insert_many(new_states)
                .execute())
        # Close older states with a single query
        if states_to_close:
            case_on_id = Case(OrderState.order_id,
                            [(state['order_id'], datetime.strptime(state['ending_at'], OrderState.ending_at.formats[0])) for state in states_to_close])
            (OrderState
                .update(ending_at=case_on_id)
                .where((OrderState.order_id.in_([state['order_id'] for state in states_to_close]) &
                        (OrderState.ending_at.is_null())))
                .execute())


def msg_to_order_dict(msg):
    def find_amount(msg):
        if msg.get('remaining_size'):
            return msg['remaining_size']
        if msg.get('new_size'):
            return msg['new_size']
        return 0

    return {
        'order_id':     msg['order_id'],
        'side':         'ask' if msg['side'] == 'sell' else 'bid',
        'product':      msg['product_id'],
                        # Returns None if price is unknown (e.g. if msg type is
                        # 'done')
        'price':        msg.get('price'),
        'amount':       find_amount(msg),
        'starting_at':  msg['time'] if msg['type'] == 'open' else None,
        'ending_at':    msg['time'] if msg['type'] == 'done' else None
    }


def msg_to_trade_dict(msg):
    return {
        'side': msg['side'],
        'amount': msg['size'],
        'product': msg['product_id'],
        'price': msg['price'],
        'time': msg['time']
    }
