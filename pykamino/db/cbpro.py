"""
Adapt Coinbase Pro's data structure to our database Models
"""

import cbpro
import itertools
from pykamino.db import Order, OrderTimeline as OTl, Trade, database

cbpro_client = cbpro.PublicClient()


class MultiProductSnapshot:
    def __init__(self, products=['BTC-USD']):
        self._snaps = {}
        for p in products:
            self._snaps[p] = Snapshot(p)
        self.sequence = -1
    
    def download(self):
        for prod in self.products:
            snap = self._snaps[prod]
            snap.download()
            if self.sequence == -1:
                # Keep the older sequence number among the updates
                self.sequence = snap.sequence
        return self.sequence

    @property
    def products(self):
        return self._snaps.keys()
    
    @property
    def snapshots(self):
        return self._snaps.values()

    def to_models(self):
        for snapshot in self.snapshots:
            for order, timeline in  snapshot.to_models():
                yield order, timeline
    
    def insert(self, clear=True):
        timelines = (Snapshot._add_order_field(book_order) for book_order in self)
        with database.atomic():
            Order.insert_many(self, fields=Order._meta.fields).on_conflict('ignore').execute()
            OTl.insert_many(timelines, fields=['price',
                                               'remaining_size',
                                               'order']).on_conflict('ignore').execute()
        if clear:
            self.clear()

    def clear(self):
        for snapshot in self.snapshots:
            snapshot.clear()

    def __iter__(self):
        for snapshot in self.snapshots:
            for book_order in snapshot:
                yield book_order


class Snapshot:
    def __init__(self, product='BTC-USD'):
        self.product = product
        self._snap = {'bid': set(), 'ask': set()}
        self.sequence = -1

    def download(self):
        cbpro_snap = cbpro_client.get_product_order_book(self.product, level=3)
        if self.sequence == -1:
            # Keep the older sequence number among the updates
            self.sequence = cbpro_snap['sequence']
        for side in (k for k in cbpro_snap if k in ['bids', 'asks']):
            # side[:-1]: Remove the trailing 's' for plural nouns (eg: asks -> ask)
            self._snap[side[:-1]].update((tuple(order) for order in cbpro_snap[side]))

    def to_models(self):
        for book_order in self:
            order = Order(**book_order)
            book_order.pop('id')
            timeline = OTl(order=order, **book_order)
            yield order, timeline
    
    @staticmethod
    def _add_order_field(book_order):
        new_order = book_order.copy()
        new_order['order'] = new_order['id']
        return new_order
    
    def insert(self, clear=True):
        timelines = (self._add_order_field(book_order) for book_order in self)
        with database.atomic():
            Order.insert_many(self, fields=Order._meta.fields).on_conflict('ignore').execute()
            OTl.insert_many(timelines, fields=['price',
                                               'remaining_size',
                                               'order']).on_conflict('ignore').execute()
        if clear:
            self.clear()

    def clear(self):
        self.sequence = -1
        for book_orders in self._snap.values():
            book_orders.clear()

    def __iter__(self):
        for side, book_orders in self._snap.items():
            for order in book_orders:
                yield {'price': order[0],
                       'remaining_size': order[1],
                       'id': order[2],
                       'product': self.product,
                       'side': side}


def msg_to_order(msg) -> (Order, OTl):
    """
    Convert a Coinbase message into an `OrderTimeline` instance, and the related `Order` instance.

    If the message is related to a market order, meaning the order has never been
    on the book, both variables will be `None`.

    Returns:
        order: an Order instance if available, else None
        timeline: an OrderTimeline instance if message is not a market order, else None
    Raises:
        ValueError: if message type is not 'change', 'done' or 'open'.
    """
    if msg['type'] not in ['change', 'done', 'open']:
        raise ValueError("Message type is not 'open', 'change', or 'done'")
    order = None
    if msg['type'] == 'open':
        order = Order(id=msg['order_id'], side=msg['side'], product=msg['product_id'])
    try:
        timeline = OTl(remaining_size=msg['remaining_size'],
                                price=msg['price'],
                                time=msg['time'],
                                order=order if order is not None else msg['order_id'],
                                reason=msg.get('reason', None))
    except KeyError:
        # from Coinbase: market orders will not have a remaining_size
        # or price field as they are never on the open order book at a given price.
        timeline = None
    return order, timeline


def msg_to_trade(msg) -> Trade:
    """
    Convert a Coinbase message into an `Trade` instance.

    Returns:
        trade: a Trade instance.
    Raises:
        ValueError: if message type is not 'match'.
    """
    if msg['type'] != 'match':
        raise ValueError("Message type is not 'match'")
    trade = Trade(side=msg['side'],
                  size=msg['size'],
                  product=msg['product_id'],
                  price=msg['price'],
                  time=msg['time'])
    return trade
