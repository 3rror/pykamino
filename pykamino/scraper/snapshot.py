from datetime import datetime

import aiohttp
from peewee import fn

from pykamino.db import OrderState, database

base_url = 'https://api.pro.coinbase.com/products/{}/book'


async def store_snapshot(product='BTC-USD'):
    with database.connection_context():
        snap = _Snapshot(product)
        await snap.sync_db()
        return snap.sequence


class _Snapshot:
    """
    An order book snapshot, i.e. the orders waiting to be filled at a given
    time.
    """

    def __init__(self, product='BTC-USD'):
        self.product = product
        self.sequence = -1
        self.temp_snapshot = create_temp_model(product)
        self.temp_snapshot.create_table(safe=False)

    async def sync_db(self):
        await self.download()
        self.close_old_states()
        self.insert_new_states()

    async def download(self):
        async with aiohttp.request('GET', base_url.format(self.product), params={'level': 3}) as response:
            assert response.status // 100 == 2
            cbpro_snap = await response.json()
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
                    # For example: asks -> ask,
                    'side': side[:-1]})
        self.temp_snapshot.insert_many(snap_list).execute()
        return self.sequence

    def close_old_states(self):
        still_open_condition = (
            (OrderState.order_id == self.temp_snapshot.order_id) &
            (OrderState.amount == self.temp_snapshot.amount))
        states_still_open = self.temp_snapshot.select().where(still_open_condition)

        (OrderState
         .update(ending_at=datetime.now())
         .where(~fn.EXISTS(states_still_open) &
                OrderState.ending_at.is_null() &
                (OrderState.product == self.product))
         .execute())

        # Remove from TempSnapshot orders that didn't change.
        # We don't need to store them again.
        (self.temp_snapshot
         .delete()
         .where(self.temp_snapshot.order_id.in_(
             self.temp_snapshot
             .select(self.temp_snapshot.order_id)
             .join(OrderState, on=(self.temp_snapshot.order_id == OrderState.order_id))
             .where(self.temp_snapshot.amount == OrderState.amount)))
         .execute())

    def insert_new_states(self, clear=True):
        """
        Store the snapshot in the database.

        This will also take care of closing orders that are not in the book
        anymore.
        """
        # Set the starting_at date for new states *after* closing the older ones.
        # This is to avoid inconsistency (previous ending_at > current starting_at)
        self.temp_snapshot.update(starting_at=datetime.now()).execute()
        OrderState.insert_from(self.temp_snapshot.select(),
                               OrderState._meta.fields).execute()


def create_temp_model(product):
    temp = type('TempSnapshot', (OrderState,), {})
    temp._meta.temporary = True
    temp._meta.table_name = f'tempsnapshot-{product}'
    return temp
