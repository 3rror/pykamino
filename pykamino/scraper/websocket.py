from datetime import datetime as dt
from queue import Queue
from threading import Condition, Event, Thread

from cbpro import WebsocketClient
from peewee import Case

from pykamino.db import OrderState, Trade
from pykamino.scraper.snapshot import store_snapshot


class Client():
    def __init__(self, buffer_len=200, products=None):
        if products is None:
            products = ['BTC-USD']
        self._seqs = {prod: -1 for prod in products}
        self._receiver = MessageReceiver(products=products)
        self._filter = MessageParser(buffer_len, sequences=self._seqs)
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
                self._seqs[p] = store_snapshot(p)
            self._filter.start()
            self._storer.start()
        else:
            raise RuntimeError('The scraper is already running.')

    def stop(self):
        self._receiver.stop()
        self._filter.stop()
        self._storer.stop()
        self._is_running = False


# Threading stuff #

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
filt_msgs_lock = Condition()
parsed_msgs = {
    'new_trades': [],
    'new_states': [],
    'closed_states': []
}


class MessageReceiver(WebsocketClient):
    def __init__(self, *args, **kwargs):
        super().__init__(channels=['full'], *args, **kwargs)

    def on_open(self):
        pass

    def on_message(self, msg):
        msg_queue.put(msg)

    def on_error(self, e, data=None):
        # Ignore the fact that we didn't receive anything (None)
        # But close the ws on other errors
        if type(e) is not ValueError:
            super().on_error(e, data)


class MessageParser(GracefulThread):
    def __init__(self, buffer_len, sequences=None):
        super().__init__()
        self.sequences = sequences
        self.buffer_len = buffer_len

    def task(self):
        msg = msg_queue.get()
        msg_queue.task_done()
        try:
            if msg['sequence'] > self.sequences[msg['product_id']]:
                with filt_msgs_lock:
                    self._parse_and_save_message(msg)
                    msg_count = sum([len(msg) for msg in parsed_msgs.values()])
                    if msg_count >= self.buffer_len:
                        filt_msgs_lock.notify_all()
        except KeyError:
            pass

    def _parse_and_save_message(self, msg):
        # All possibile message types are:
        # activate, received, match, open, change, done

        if msg['type'] == 'activate' or msg['type'] == 'received':
            # We skip them because they don't change the orderbook
            return
        elif msg['type'] == 'match':
            self._append_trade_message(msg)
        elif msg['type'] == 'open':
            self._append_open_order_message(msg)
        elif msg['type'] == 'change':
            self._append_changed_order_message(msg)
        elif msg['type'] == 'done':
            self._append_close_order_message(msg)

    def _append_trade_message(self, msg):
        # Match message example:
        # {
        #     "type": "match",
        #     "trade_id": 10,
        #     "sequence": 50,
        #     "maker_order_id": "ac928c66-ca53-498f-9c13-a110027a60e8",
        #     "taker_order_id": "132fb6ae-456b-4654-b4e0-d681ac05cea1",
        #     "time": "2014-11-07T08:19:27.028459Z",
        #     "product_id": "BTC-USD",
        #     "size": "5.23512",
        #     "price": "400.23",
        #     "side": "sell"
        # }
        parsed_msgs['new_trades'].append({
            'side': msg['side'],
            'amount': msg['size'],
            'product': msg['product_id'],
            'price': msg['price'],
            'time': msg['time']
        })

    def _append_open_order_message(self, msg):
        # Open message example
        # {
        #     "type": "open",
        #     "time": "2014-11-07T08:19:27.028459Z",
        #     "product_id": "BTC-USD",
        #     "sequence": 10,
        #     "order_id": "d50ec984-77a8-460a-b958-66f114b0de9b",
        #     "price": "200.2",
        #     "remaining_size": "1.00",
        #     "side": "sell"
        # }
        parsed_msgs['new_states'].append({
            'order_id': msg['order_id'],
            'product': msg['product_id'],
            'side': msg['side'],
            'price': msg['price'],
            'amount': msg['remaining_size'],
            'starting_at': msg['time']
        })

    def _append_changed_order_message(self, msg):
        # Change order message example:
        # {
        #     "type": "change",
        #     "time": "2014-11-07T08:19:27.028459Z",
        #     "sequence": 80,
        #     "order_id": "ac928c66-ca53-498f-9c13-a110027a60e8",
        #     "product_id": "BTC-USD",
        #     "new_size": "5.23512",
        #     "old_size": "12.234412",
        #     "price": "400.23",
        #     "side": "sell"
        # }
        if 'new_funds' in msg:
            # We have a market order, skip it
            return

        parsed_msgs['closed_states'].append({
            'order_id': msg['order_id'],
            'ending_at': msg['time']
        })

        parsed_msgs['new_states'].append({
            'order_id': msg['order_id'],
            'product': msg['product_id'],
            'side': msg['side'],
            'price': msg['price'],
            'amount': msg['new_size'],
            'starting_at': msg['time']
        })

    def _append_close_order_message(self, msg):
        # Done message example
        # {
        #     "type": "done",
        #     "time": "2014-11-07T08:19:27.028459Z",
        #     "product_id": "BTC-USD",
        #     "sequence": 10,
        #     "price": "200.2",
        #     "order_id": "d50ec984-77a8-460a-b958-66f114b0de9b",
        #     "reason": "filled", // or "canceled"
        #     "side": "sell",
        #     "remaining_size": "0"
        # }

        # Market orders will not have a remaining_size or price field as they
        # are never on the open order book at a given price.
        if 'remaining_size' not in msg or 'price' not in msg:
            # We have a market order, skip it
            return
        parsed_msgs['closed_states'].append({
            'order_id': msg['order_id'],
            'ending_at': msg['time']
        })


class MessageStorer(GracefulThread):
    def task(self):
        with filt_msgs_lock:
            filt_msgs_lock.wait()
            self.store_messages()
            self._clean_parsed_msgs()

    def _clean_parsed_msgs(self):
        for key in parsed_msgs:
            parsed_msgs[key] = []

    def store_messages(self):
        if parsed_msgs['new_trades']:
            self._add_new_trades()
        if parsed_msgs['closed_states']:
            self._close_old_states()
        if parsed_msgs['new_states']:
            self._add_new_states()

    def _add_new_trades(self):
        (Trade
            .insert_many(parsed_msgs['new_trades'])
            .execute())

    def _close_old_states(self):
        def formatted_time(ending_at):
            return dt.strptime(ending_at, OrderState.ending_at.formats[0])

        substitutions = []
        for state in parsed_msgs['closed_states']:
            substitutions.append(
                (state['order_id'], formatted_time(state['ending_at'])))
        ids = [state['order_id'] for state in parsed_msgs['closed_states']]
        # We want to generate a single update query, so we use the case
        # statement to specify the correct new values
        (OrderState
            .update(ending_at=Case(OrderState.order_id, substitutions))
            .where((OrderState.order_id.in_(ids) &
                    (OrderState.ending_at.is_null())))
            .execute())

    def _add_new_states(self):
        (OrderState
            .insert_many(parsed_msgs['new_states'])
            .execute())
