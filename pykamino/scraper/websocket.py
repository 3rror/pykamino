import asyncio
import multiprocessing
from datetime import datetime as dt
from time import sleep

import aiohttp
# iso8601 is needed as fromisoformat() has been added in Python 3.7
import iso8601
from peewee import Case

from pykamino.db import OrderState, Trade, database
from pykamino.scraper.snapshot import store_snapshot

coinbase_feed = 'wss://ws-feed.pro.coinbase.com'


class Client():
    def __init__(self, buffer_len=None, products=('BTC-USD',), session=None):
        self.products = products
        self.buf_len = 300*len(products) if buffer_len is None else buffer_len
        self._private_session = not bool(session)
        if not self._private_session:
            self.session = session
        self.storer_rx, self.storer_tx = multiprocessing.Pipe(duplex=False)
        self.storer = MessageStorer(self.storer_rx)

    async def coro(self):
        """
        Coroutine to initialize and listen to the websocket.
        """
        try:
            self.storer.start()
            self.ws, *seqs = await asyncio.gather(self._init_ws(coinbase_feed),
                                                  *[store_snapshot(p) for p in self.products])
            parser = MessageParser(
                dict(zip(self.products, seqs)), self.buf_len)
            async for message in self.ws:
                if message.type == aiohttp.WSMsgType.TEXT:
                    parser.parse(message.json())
                    if parser.message_count() >= self.buf_len:
                        self._send_to_storer(parser)
                elif message.type == aiohttp.WSMsgType.ERROR:
                    break
        except asyncio.CancelledError:
            # If we got CancelledError, don't freak out! It's all OK
            return
        finally:
            await self.close()

    async def close(self):
        if self.ws is not None:
            await self.ws.close()
        if self._private_session:
            await self.session.close()
        self.storer.close()
        self.storer.join()

    async def _init_ws(self, url, *args, **kwargs):
        feed_conf = {'type': 'subscribe', 'channels': ['full'],
                     'product_ids': self.products}
        if self._private_session:
            self.session = aiohttp.ClientSession()
        ws = await self.session.ws_connect(url, *args, **kwargs)
        await ws.send_json(feed_conf)
        return ws

    def _send_to_storer(self, parser):
        self.storer_tx.send(parser.messages.copy())
        parser.clear()


class MessageParser:
    """
    Parse messages received from Coinbase Pro's websocket
    and internally store them in categories:
     - new trades
     - new states
     - changed states
     - closed states
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
            # The first message is different: it has no 'sequence'
            if msg['sequence'] > self.sequences[msg['product_id']]:
                self.classify(msg)
        except KeyError:
            pass

    def message_count(self):
        return sum((len(lst) for lst in self.messages.values()))

    def clear(self):
        for lst in self.messages.values():
            lst.clear()

    def classify(self, msg):
        # All possibile message types are:
        # activate, received, match, open, change, done

        if msg['type'] == 'activate' or msg['type'] == 'received':
            # We skip them because they don't change the orderbook
            return

        # HACK: use system's clock because CB returns a wrongly formatted
        # ISO8601 datetime.
        # https://github.com/coinbase/coinbase-pro-node/issues/358
        msg['time'] = dt.utcnow().isoformat()

        if msg['type'] == 'match':
            self._append_to_trades(msg)
        elif msg['type'] == 'open':
            self._append_to_open_orders(msg)
        elif msg['type'] == 'done':
            self._append_to_closed_orders(msg)
        elif msg['type'] == 'change':
            self._append_to_changed_orders(msg)

    def _append_to_trades(self, msg):
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

    def _append_to_open_orders(self, msg):
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

    def _append_to_changed_orders(self, msg):
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

        # Any change message where the price is null indicates that the change
        # message is for a market order. Change messages for limit orders will
        # always have "price" defined.
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

    def _append_to_closed_orders(self, msg):
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

    def run(self):
        while not self.stop_event.is_set():
            try:
                if self.conn.poll(timeout=0.5):
                    try:
                        self.messages = self.conn.recv()
                    except EOFError:
                        # The other end has been closed. There is reason
                        # To keep this process alive
                        break
                    else:
                        self.store_messages()
                        self.messages = {}
            except KeyboardInterrupt:
                self.close()

    def close(self):
        self.stop_event.set()

    def store_messages(self):
        with database:
            if self.messages['new_trades']:
                self._add_new_trades()
            if self.messages['new_states']:
                self._add_new_states()
            if self.messages['changed_states']:
                self._update_states()
            if self.messages['closed_states']:
                self._close_states()

    def _add_new_trades(self):
        (Trade
            .insert_many(self.messages['new_trades'])
            .execute())

    def _add_new_states(self):
        (OrderState
            .insert_many(self.messages['new_states'])
            .execute())

    def _update_states(self):
        # Changed orders are rare, so we can afford to spawn 3 queries per order
        for state in self.messages['changed_states'][:]:
            query = (OrderState
                     .select()
                     .where((OrderState.order_id == state['order_id']) &
                            (OrderState.starting_at < iso8601.parse_date(state['time']))))
            if query.exists():
                (OrderState
                    .update(ending_at=state['time'])
                    .where((OrderState.order_id == state['order_id']) &
                           (OrderState.ending_at.is_null()))
                    .execute())
                state['starting_at'] = state['time']
                del state['time']
                (OrderState.insert(state).execute())

    def _close_states(self):
        substitutions = []
        ids = []
        for state in self.messages['closed_states']:
            ids.append(state['order_id'])
            substitutions.append(
                (state['order_id'], iso8601.parse_date(state['ending_at'])))
        # We want to generate a single update query, so we use the case
        # statement to specify the correct new values
        (OrderState
            .update(ending_at=Case(OrderState.order_id, substitutions))
            .where((OrderState.order_id.in_(ids)) &
                   (OrderState.ending_at.is_null()))
            .execute())
