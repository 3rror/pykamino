from pykamino.db import database, Trade
import cbpro


class Scraper(cbpro.WebsocketClient):
    CACHE_SIZE = 100

    def __init__(self, *args, **kwargs):
        super().__init__(channels=['matches'], *args, **kwargs)

    def on_open(self):
        self.messages = []

    def on_message(self, msg):
        self.messages.append(msg)
        if len(self.messages) == Scraper.CACHE_SIZE:
            with database.atomic():
                Trade.insert_many(list(self.matches(self.messages)),
                                  fields=Trade._meta.fields).execute()
            self.messages.clear()

    def on_close(self):
        pass
        # TODO: parse the remaining data in the list, then empty it

    def matches(self, msg_list):
        return (msg for msg in msg_list if msg['type'] == 'last_match' or msg['type'] == 'match')
