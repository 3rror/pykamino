from datetime import datetime

import cbpro
from peewee import UUIDField, fn

from pykamino.db import BaseModel, Order
from pykamino.db import OrderHistory as History
from pykamino.db import Trade, database

cbpro_client = cbpro.PublicClient()


class Snapshot:
    """
    An order book snapshot, i.e. the orders waiting to be filled at a given time.
    """

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
                                   'amount': order[1],
                                   'id': order[2],
                                   'product': self.product,
                                   'side': side[:-1]})
        return self.sequence

    def to_models(self):
        """
        An iterator over every order in the book, yielding
        a pair of database models.

        Returns:
            a tuple at every iteration, composed of an Order instance
            and a Timeline instance.
        """
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
        self.TempSnapshot.create_table()
        self.TempSnapshot.insert_many(self, fields=['id']).execute()
        in_book = self.TempSnapshot.select().where(self.TempSnapshot.id == Order.id)
        Order.update(close_time=datetime.now()).where(
            ~fn.EXISTS(in_book), Order.close_time == None).execute()
        # Empty the temporary table
        self.TempSnapshot.raw('TRUNCATE TABLE temp_snapshot')

    def insert(self, clear=True):
        """
        Store the snapshot in the database.
        This will also take care of closing orders that are not in the book anymore.
        """
        timelines = (self._add_order_field(book_order) for book_order in self)
        with database.atomic():
            self._close_old_orders()
            with database.atomic():
                Order.insert_many(self, fields=['id', 'side', 'price', 'product']).on_conflict(
                    'ignore').execute()
                History.insert_many(timelines, fields=['amount', 'order']).on_conflict(
                    'ignore').execute()
        if clear:
            self.clear()

    def clear(self):
        """
        Get rid of the previously downloaded snapshot.
        """
        self.sequence = -1
        self._snap.clear()

    def __iter__(self):
        return iter(self._snap)

    class TempSnapshot(BaseModel):
        """
        A temporary table in lieu of a `NOT IN` clause for performance reasons.
        Its purpose is to check what open orders are in the database, but not in the
        snapshot, so that we know these orders, in reality, are now in a closed state.
        """
        id = type(Order.id)(primary_key=True)

        class Meta:
            temporary = True
