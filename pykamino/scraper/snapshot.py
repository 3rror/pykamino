from datetime import datetime

import cbpro
from peewee import fn

from pykamino.db import BaseModel, OrderState

cbpro_client = cbpro.PublicClient()


class Snapshot:
    """
    An order book snapshot, i.e. the orders waiting to be filled at a given
    time.
    """

    def __init__(self, product='BTC-USD'):
        self.product = product
        self._snap = []
        self.sequence = -1

    def __iter__(self):
        return iter(self._snap)

    def download(self):
        cbpro_snap = cbpro_client.get_product_order_book(self.product, level=3)
        self.sequence = cbpro_snap['sequence']
        for side in ('bids', 'asks'):
            for order_msg in cbpro_snap[side]:
                self._snap.append({'price': order_msg[0],
                                   'amount': order_msg[1],
                                   'order_id': order_msg[2],
                                   'product': self.product,
                                   # Remove the trailing 's' for plural nouns
                                   # eg: asks -> ask
                                   'side': side[:-1]})
        return self.sequence

    def insert(self, clear=True):
        """
        Store the snapshot in the database.

        This will also take care of closing orders that are not in the book
        anymore.
        """
        self._close_old_orders()
        fields_to_save = ['order_id', 'product',
                          'side', 'price', 'amount', 'starting_at']
        OrderState.insert_many(self, fields=fields_to_save).execute()
        if clear:
            self.clear()

    def clear(self):
        """
        Get rid of the previously downloaded snapshot.
        """
        self.sequence = -1
        self._snap.clear()

    def to_models(self):
        """
        An iterator over every order in the snapshot, yielding a database
        model.
        """
        for book_order in self:
            yield OrderState({**book_order, 'starting_at': datetime.now()})

    def _close_old_orders(self):
        self.TempSnapshot.create_table()
        (self.TempSnapshot
            .insert_many(self, fields=['order_id', 'amount'])
            .execute())
        condition = (
            (self.TempSnapshot.order_id == OrderState.order_id) &
            (OrderState.ending_at.is_null()) &
            (self.TempSnapshot.amount == OrderState.amount))
        states_open = self.TempSnapshot.select().where(condition)
        (OrderState
            .update(ending_at=datetime.now())
            .where(~fn.EXISTS(states_open) & OrderState.ending_at.is_null())
            .execute())
        # Remove the temporary table
        self.TempSnapshot.drop_table()

    class TempSnapshot(BaseModel):
        """
        A temporary table in lieu of a `NOT IN` clause for performance reasons.
        Its purpose is to check what open orders are in the database, but not
        in the snapshot, so that we know these orders, in reality, are now in a
        closed state.
        """
        order_id = type(OrderState.order_id)(primary_key=True)
        amount = type(OrderState.amount)()

        class Meta:
            temporary = True
