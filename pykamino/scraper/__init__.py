from contextlib import suppress
from peewee import ProgrammingError
from pykamino.db import database, dict_to_orders, Trade, Order, OrderTimeline
import cbpro
import itertools


class Scraper(cbpro.WebsocketClient):
    BUFFER_SIZE = 100

    def __init__(self, *args, **kwargs):
        super().__init__(channels=['full'], *args, **kwargs)

    def on_open(self):
        self.messages = []

    def on_message(self, msg):
        self.messages.append(msg)
        if len(self.messages) == Scraper.BUFFER_SIZE:
            orders, trades = self.classify_messages(self.messages)
            orders, timelines = dict_to_orders(orders)
            with database.atomic():
                with suppress(ProgrammingError):
                    Trade.insert_many(trades, fields=Trade._meta.fields).execute()
                    Order.bulk_create(orders)
                with database.atomic():
                    with suppress(Order.DoesNotExist):
                        import pdb
                        pdb.set_trace()
                        OrderTimeline.bulk_create(timelines)
            self.messages.clear()

    def on_close(self):
        pass
        # TODO: parse the remaining data in the list, then empty it

    def classify_messages(self, msg_list):
        """
        Split the list of messages in two iterators: orders and trades
        """
        orders, trades = itertools.tee(msg_list)

        def pred(msg): return msg['type'] == 'match'
        return itertools.filterfalse(pred, orders), filter(pred, trades)
