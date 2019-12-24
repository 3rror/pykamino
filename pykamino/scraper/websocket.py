import asyncio
import multiprocessing
import sys
from datetime import datetime
from time import sleep
from typing import Optional, Tuple

import aiohttp
from peewee import Case

from pykamino.db import OrderState, Trade, database
from pykamino.scraper import snapshot

coinbase_feed = 'wss://ws-feed.pro.coinbase.com'


class Client:
    def __init__(self,
                 url: str = coinbase_feed,
                 buffer_len: Optional[int] = None,
                 products: Tuple[str, ...] = ('BTC-USD',),
                 session: Optional[aiohttp.ClientSession] = None):
        self.url = url
        self.buf_len = 300*len(products) if buffer_len is None else buffer_len
        self.products = products
        self.has_private_session = not bool(session)
        if not self.has_private_session:
            self.session = session
        storer_rx, self.storer_tx = multiprocessing.Pipe(duplex=False)
        self.storer = MessageStorer(storer_rx)

    async def start(self) -> None:
        """
        Coroutine to initialize and listen to the websocket.
        """
        try:
            self.storer.start()
            self.ws, *seqs = await asyncio.gather(self.init_ws(self.url),
                                                  *[snapshot.store(p) for p in self.products])
            parser = MessageParser(
                dict(zip(self.products, seqs)), self.buf_len)
            async for message in self.ws:
                if message.type == aiohttp.WSMsgType.TEXT:
                    parser.parse(message.json())
                    if parser.message_count() >= self.buf_len:
                        self.send_to_storer(parser)
                elif message.type == aiohttp.WSMsgType.ERROR:
                    break
        except asyncio.CancelledError:
            # If we got CancelledError from somewhere, it's because something
            # wants to close the socket.
            return
        finally:
            await self.close()

    async def close(self) -> None:
        if self.ws is not None:
            await self.ws.close()
        if self.has_private_session:
            await self.session.close()
        self.storer.close()
        self.storer.join()

    async def init_ws(self, *args, **kwargs):
        feed_conf = {'type': 'subscribe', 'channels': ['full'],
                     'product_ids': self.products}
        if self.has_private_session:
            self.session = aiohttp.ClientSession()
        ws = await self.session.ws_connect(*args, **kwargs)
        await ws.send_json(feed_conf)
        return ws

    def send_to_storer(self, parser):
        self.storer_tx.send(parser.messages.copy())
        parser.clear()


class MessageParser:
    """
    Parse messages received from Coinbase Pro's websocket
    and internally store them in categories:
     - New trades
     - New states
     - Changed states
     - Closed states
    """

    def __init__(self, sequences, buffer_len=200):
        self.sequences = sequences
        self.buffer_len = buffer_len
        self.messages = {
            'new_trades': [],
            'new_states': [],
            'changed_states': [],
            'closed_states': []}

    def parse(self, msg):
        try:
            if msg['sequence'] > self.sequences[msg['product_id']]:
                self.classify(msg)
        except KeyError:
            # The first message is different: it has no 'sequence'. Ignore it.
            pass

    def message_count(self):
        return sum((len(lst) for lst in self.messages.values()))

    def clear(self):
        for lst in self.messages.values():
            lst.clear()

    def classify(self, msg):
        if msg['type'] == 'activate' or msg['type'] == 'received':
            # We skip them because they don't change the orderbook
            return

        # HACK: use system's clock because CB returns a wrongly formatted
        # ISO8601 datetime. (https://github.com/coinbase/coinbase-pro-node/issues/358)
        # Even though the issue is currently fixed, I don't trust that.
        msg['time'] = datetime.now()

        msg_type = msg['type']
        if msg_type == 'match':
            self.append_to_trades(msg)
        elif msg_type == 'open':
            self.append_to_new_states(msg)
        elif msg_type == 'change':
            self.append_to_changed_states(msg)
        elif msg_type == 'done':
            self.append_to_closed_states(msg)

    def append_to_trades(self, msg):
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
        self.messages['new_trades'].append({
            'side': msg['side'],
            'amount': msg['size'],
            'product': msg['product_id'],
            'price': msg['price'],
            'time': msg['time']
        })

    def append_to_new_states(self, msg):
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
        self.messages['new_states'].append({
            'order_id': msg['order_id'],
            'product': msg['product_id'],
            'side': 'ask' if msg['side'] == 'sell' else 'bid',
            'price': msg['price'],
            'amount': msg['remaining_size'],
            'starting_at': msg['time']
        })

    def append_to_changed_states(self, msg):
        # Change message example:
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

        # Any change message where the price is None indicates that the change
        # message is for a market order.
        # Change messages for limit orders will always have "price" defined.
        if msg['price'] is None:
            return

        self.messages['changed_states'].append({
            'order_id': msg['order_id'],
            'product': msg['product_id'],
            'side': 'ask' if msg['side'] == 'sell' else 'bid',
            'price': msg['price'],
            'amount': msg['new_size'],
            'time': msg['time']
        })

    def append_to_closed_states(self, msg):
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
            return
        self.messages['closed_states'].append({
            'order_id': msg['order_id'],
            'ending_at': msg['time']})


class MessageStorer(multiprocessing.Process):
    """
    A process that waits for a list of parsed messages and
    then stores them in parallel.
    """

    def __init__(self, conn):
        super().__init__()
        self.stop_event = multiprocessing.Event()
        self.conn = conn
        self.messages = {}

    # Overridden
    def run(self):
        while not self.stop_event.is_set():
            try:
                if self.conn.poll(timeout=0.5):
                    try:
                        self.messages = self.conn.recv()
                    except EOFError:
                        # The other end has been closed. There is reason
                        # To keep this process alive.
                        break
                    else:
                        self.store_messages()
                        self.messages = {}
            except KeyboardInterrupt:
                self.close()

    # Overridden only in Python 3.7+
    def close(self):
        self.stop_event.set()
        if sys.version_info >= (3, 7):
            super().close()

    def store_messages(self):
        with database:
            if self.messages['new_trades']:
                self.add_new_trades()
            if self.messages['new_states']:
                self.add_new_states()
            if self.messages['changed_states']:
                self.update_states()
            if self.messages['closed_states']:
                self.close_states()

    def add_new_trades(self):
        (Trade
            .insert_many(self.messages['new_trades'])
            .execute())

    def add_new_states(self):
        (OrderState
            .insert_many(self.messages['new_states'])
            .execute())

    def update_states(self):
        # Changed orders are rare, so we can afford to spawn 3 queries per order
        for state in self.messages['changed_states'][:]:
            stored_state = (OrderState
                            .select()
                            .where((OrderState.order_id == state['order_id']) &
                                   (OrderState.starting_at < state['time'])))
            if stored_state.exists():
                (OrderState
                    .update(ending_at=state['time'])
                    .where((OrderState.order_id == state['order_id']) &
                           (OrderState.ending_at.is_null()))
                    .execute())
                state['starting_at'] = state['time']
                del state['time']
                (OrderState.insert(state).execute())

    def close_states(self):
        substitutions = []
        ids = []
        for state in self.messages['closed_states']:
            ids.append(state['order_id'])
            substitutions.append(
                (state['order_id'], state['ending_at']))
        # We want to generate a single update query, so we use the case
        # statement to specify the correct new values
        (OrderState
            .update(ending_at=Case(OrderState.order_id, substitutions))
            .where((OrderState.order_id.in_(ids)) &
                   (OrderState.ending_at.is_null()))
            .execute())
