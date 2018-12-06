from contextlib import suppress
from peewee import ProgrammingError
from pykamino.db import database, Trade
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
            with database.atomic():
                with suppress(ProgrammingError):
                    Trade.insert_many(trades, fields=Trade._meta.fields).execute()
                if orders:
                    pass
            self.messages.clear()

    def on_close(self):
        pass
        # TODO: parse the remaining data in the list, then empty it

    def classify_messages(self, msg_list):
        """
        Split the list of messages in two iterators: orders and trades
        """
        t1, t2 = itertools.tee(msg_list)

        def pred(msg): return msg['type'] == 'match'
        return itertools.filterfalse(pred, t1), filter(pred, t2)


def handle_order_types(orders):
    for order in orders:
        if order['type'] == 'open':
            pass
