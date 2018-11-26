import cbpro


class Scraper(cbpro.WebsocketClient):
    CACHE_SIZE = 1000

    def __init__(self, *args, **kwargs):
        super().__init__(channels=['full'], *args, **kwargs)

    def on_open(self):
        self.messages = []

    def on_message(self, msg):
        if len(self.messages) == Scraper.CACHE_SIZE:
            # TODO: parse things here
            self.messages.clear()
        else:
            self.messages.append(msg)

    def on_close(self):
        pass
        # TODO: parse the remaining data in the list, then empty it
