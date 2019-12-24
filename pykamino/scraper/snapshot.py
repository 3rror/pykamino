from datetime import datetime
from typing import Any, Dict, Iterator, List

from peewee import fn
from pykamino.db import OrderState, database
import aiohttp

base_url = 'https://api.pro.coinbase.com/products/{}/book'


async def store(product='BTC-USD') -> int:
    """
    Download and store the order book snapshot for a particular product.

    This is a helper function to download and save a snapshot at once.
    For a more fine-grained behavior, consider to use the `Snapshot` and `Storer` classes alone.

    Returns:
        the sequence number for that product.
    """
    snap = OrderBook(product)
    await snap.download()
    storer = Storer(snap)
    with database:
        storer.close_old_states()
        storer.insert_new_states()
    return snap.sequence


class OrderBook:
    def __init__(self, product='BTC-USD'):
        self.product = product
        self.sequence = None
        self.timestamp = None
        self.orders = None

    async def download(self) -> int:
        """
        Download the current order book from Coinbase Pro. If no error occurs, the `sequence`
        and `timestamp` values will be assigned.

        Returns:
            The sequence number.
        """
        async with aiohttp.request('GET', base_url.format(self.product), params={'level': 3},
                                   raise_for_status=True, compress=True) as response:
            cbpro_snap = await response.json()
        self.timestamp = datetime.now()
        self.sequence = cbpro_snap['sequence']
        del cbpro_snap['sequence']
        self.orders = cbpro_snap
        return self.sequence

    def describe_order(self, order: List[Any], side: str) -> Dict[str, Any]:
        return {'price': order[0],
                'amount': order[1],
                'order_id': order[2],
                'product': self.product,
                'side': side}

    def bids(self) -> Iterator[Dict[str, Any]]:
        """
        If `download()` has been called, get all the "bid" orders.

        Returns:
            An iterator over the "bid" orders.
        """
        return (self.describe_order(order, 'bid') for order in self.orders['bids'])

    def asks(self) -> Iterator[Dict[str, Any]]:
        """
        If `download()` has been called, get all the "ask" orders.

        Returns:
            An iterator over the "ask" orders.
        """
        return (self.describe_order(order, 'ask') for order in self.orders['asks'])

    def __iter__(self) -> Iterator[Dict[str, Any]]:
        yield from self.bids()
        yield from self.asks()


class Storer:
    """
    Utility to save a downloaded OrderBook to pykamino's database.
    """
    def __init__(self, order_book: OrderBook):
        def get_temp_model():
            temp = type('TempOrderState', (OrderState,), {})
            temp._meta.temporary = True
            temp._meta.table_name = f'tempbook-{id(self)}'
            return temp
        self.timestamp = order_book.timestamp
        self.product = order_book.product
        self.temp_order_state = get_temp_model()
        self.temp_order_state.create_table()
        self.temp_order_state.insert_many(order_book).execute()

    def close_old_states(self) -> None:
        with database:
            states_still_open = (self.temp_order_state
                                 .select()
                                 .where(((OrderState.order_id == self.temp_order_state.order_id)
                                         & (OrderState.amount == self.temp_order_state.amount))))
            (OrderState
             .update(ending_at=self.timestamp)
             .where(~fn.EXISTS(states_still_open) &
                    OrderState.ending_at.is_null() &
                    (OrderState.product == self.product))
             .execute())

            # Remove from TempSnapshot orders that didn't change, so that
            # We don't need to store them again.
            (self.temp_order_state
             .delete()
             .where(self.temp_order_state.order_id.in_(
                 self.temp_order_state
                 .select(self.temp_order_state.order_id)
                 .join(OrderState, on=(self.temp_order_state.order_id == OrderState.order_id))
                 .where(self.temp_order_state.amount == OrderState.amount)))
             .execute())

    def insert_new_states(self, clear=True) -> None:
        self.temp_order_state.update(starting_at=self.timestamp).execute()
        OrderState.insert_from(self.temp_order_state.select(),
                               OrderState._meta.fields).execute()
