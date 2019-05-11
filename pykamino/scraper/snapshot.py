from datetime import datetime

from cbpro import PublicClient
from peewee import fn

from pykamino.db import BaseModel, OrderState


def store_snapshot(product='BTC-USD'):
    snap = _Snapshot(product)
    snap.sync_db()
    return snap.sequence


class _Snapshot:
    """
    An order book snapshot, i.e. the orders waiting to be filled at a given
    time.
    """

    def __init__(self, product='BTC-USD'):
        self.product = product
        self.sequence = -1
        _TempSnapshot.create_table()

    def sync_db(self):
        self.download()
        self.close_old_states()
        self.insert_new_states()

    def download(self):
        cbpro_snap = PublicClient().get_product_order_book(self.product, level=3)
        self.sequence = cbpro_snap['sequence']
        snap_list = []
        for side in ('bids', 'asks'):
            for order_msg in cbpro_snap[side]:
                snap_list.append({
                    'price': order_msg[0],
                    'amount': order_msg[1],
                    'order_id': order_msg[2],
                    'product': self.product,
                    # Remove the trailing 's' for plural nouns
                    # For example: asks -> ask
                    'side': side[:-1]})
        _TempSnapshot.insert_many(snap_list).execute()
        return self.sequence

    def close_old_states(self):
        still_open_condition = (
            (OrderState.order_id == _TempSnapshot.order_id) &
            (OrderState.amount == _TempSnapshot.amount))
        states_still_open = _TempSnapshot.select().where(still_open_condition)

        (OrderState
         .update(ending_at=datetime.now())
         .where(~fn.EXISTS(states_still_open) & OrderState.ending_at.is_null())
         .execute())

        # Remove from _TempSnapshot orders that didn't change.
        # We don't need to store them again.
        (_TempSnapshot
         .delete()
         .where(_TempSnapshot.order_id.in_(
             _TempSnapshot
             .select(_TempSnapshot.order_id)
             .join(OrderState, on=(_TempSnapshot.order_id == OrderState.order_id))
             .where(_TempSnapshot.amount == OrderState.amount)))
         .execute())

    def insert_new_states(self, clear=True):
        """
        Store the snapshot in the database.

        This will also take care of closing orders that are not in the book
        anymore.
        """
        # Set the starting_at date for new states *after* closing the older ones.
        # This is to avoid inconsistency (previous ending_at > current starting_at)
        _TempSnapshot.update(starting_at=datetime.now()).execute()
        OrderState.insert_from(_TempSnapshot.select(),
                               OrderState._meta.fields).execute()
        _TempSnapshot.drop_table()


class _TempSnapshot(OrderState):
    """
    A temporary table in lieu of a `NOT IN` clause for performance reasons.
    Its purpose is to check what open orders are in the database but not
    in the snapshot, so that we know these orders are now in a
    closed state.
    """

    class Meta:
        temporary = True
