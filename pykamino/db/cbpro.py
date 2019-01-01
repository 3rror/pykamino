"""
Adapt Coinbase Pro's data structure to our database Models
"""

from datetime import datetime

import cbpro
from peewee import UUIDField, fn

from pykamino.db import BaseModel, Order
from pykamino.db import OrderHistory as History
from pykamino.db import Trade, database

cbpro_client = cbpro.PublicClient()

class Snapshot:
    def __init__(self, product='BTC-USD'):
        self.product = product
        self._snap = []
        self.sequence = -1

    def download(self):
        cbpro_snap = cbpro_client.get_product_order_book(self.product, level=3)
        self.sequence = cbpro_snap['sequence']
        for side in (k for k in cbpro_snap if k in ['bids', 'asks']):
            for order in cbpro_snap[side]:
                # side[:-1]: Remove the trailing 's' for plural nouns (eg: asks -> ask)
                self._snap.append({'price': order[0],
                                   'size': order[1],
                                   'id': order[2],
                                   'product': self.product,
                                   'side': side[:-1]})
        return self.sequence

    def to_models(self):
        for book_order in self:
            order = Order(**book_order)
            book_order.pop('id')
            timeline = History(order=order, **book_order)
            yield order, timeline
    
    @staticmethod
    def _add_order_field(book_order):
        new_order = book_order.copy()
        new_order['order'] = new_order['id']
        return new_order

    def _close_old_orders(self):
        self.TempSnapshot.insert_many(self, fields=['id']).execute()
        in_book = self.TempSnapshot.select().where(self.TempSnapshot.id==Order.id)
        Order.update(close_time=datetime.now()).where(~fn.EXISTS(in_book), Order.close_time==None).execute()
        self.TempSnapshot.raw('TRUNCATE TABLE temp_snapshot')
    
    def insert(self, clear=True):
        timelines = (self._add_order_field(book_order) for book_order in self)
        with database.atomic():
            self._close_old_orders()
            with database.atomic():
                Order.insert_many(self, fields=['id', 'side', 'price', 'product']).on_conflict('ignore').execute()
                History.insert_many(timelines, fields=['size', 'order']).on_conflict('ignore').execute()
        if clear:
            self.clear()

    def clear(self):
        self.sequence = -1
        self._snap.clear()

    def __iter__(self):
        return iter(self._snap)


    class TempSnapshot(BaseModel):
        id = UUIDField(primary_key=True)

        class Meta:
            temporary = True


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
