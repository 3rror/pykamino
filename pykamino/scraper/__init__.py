from pykamino.db.models import Trade
import cbpro


class Scraper(cbpro.WebsocketClient):
    CACHE_SIZE = 100

    def __init__(self, db_conn, *args, **kwargs):
        super().__init__(channels=['matches'], *args, **kwargs)
        self.db_conn = db_conn

    def on_open(self):
        self.messages = []

    def on_message(self, msg):
        if len(self.messages) == Scraper.CACHE_SIZE:
            Trade.insert_many(list(self.matches(self.messages)))
            self.messages.clear()
        else:
            self.messages.append(msg)
        print(msg)

    def on_close(self):
        pass
        # TODO: parse the remaining data in the list, then empty it

    def matches(self, msg_list):
        return (msg for msg in msg_list if msg['type'] == 'last_match' or msg['type'] == 'match')
