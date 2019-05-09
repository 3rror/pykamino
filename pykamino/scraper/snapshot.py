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

    class TempSnapshot(OrderState):
        """
        A temporary table in lieu of a `NOT IN` clause for performance reasons.
        Its purpose is to check what open orders are in the database, but not
        in the snapshot, so that we know these orders, in reality, are now in a
        closed state.
        """

        class Meta:
            temporary = True

    def __init__(self, product='BTC-USD'):
        self.product = product
        self.TempSnapshot.create_table()

    def download(self):
        cbpro_snap = cbpro_client.get_product_order_book(self.product, level=3)
        self.sequence = cbpro_snap['sequence']
        snap_list = []
        for side in ('bids', 'asks'):
            for order_msg in cbpro_snap[side]:
                snap_list.append({
                    'price': order_msg[0],
                    'amount': order_msg[1],
                    'order_id': order_msg[2],
                    'product': self.product,
                    'starting_at': datetime.now(),
                    # Remove the trailing 's' for plural nouns
                    # For example: asks -> ask
                    'side': side[:-1]})
        self.TempSnapshot.insert_many(snap_list).execute()
        return self.sequence

    def store(self, clear=True):
        """
        Store the snapshot in the database.

        This will also take care of closing orders that are not in the book
        anymore.
        """
        self._close_old_orders()
        OrderState.insert_from(self.TempSnapshot.select(),
                               OrderState._meta.fields).execute()
        self.TempSnapshot.drop_table()

    def _close_old_orders(self):
        still_open_condition = (
            (OrderState.order_id == self.TempSnapshot.order_id) &
            (OrderState.ending_at.is_null()) &
            (OrderState.amount == self.TempSnapshot.amount))
        states_still_open = self.TempSnapshot.select().where(still_open_condition)

        (OrderState
            .update(ending_at=datetime.now())
            .where(~fn.EXISTS(states_still_open) & OrderState.ending_at.is_null())
            .namedtuples())
        # Remove orders that we have already filtered away
        (self.TempSnapshot
            .delete()
            .where(
                self.TempSnapshot.order_id.in_(
                    self.TempSnapshot
                        .select(self.TempSnapshot.order_id)
                        .join(OrderState, on=(self.TempSnapshot.order_id == OrderState.order_id))
                        .where(self.TempSnapshot.amount == OrderState.amount)))
            .execute())
