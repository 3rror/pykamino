import cbpro


class Scraper(cbpro.WebsocketClient):
    CACHE_SIZE = 1000

    def __init__(self, *args, **kwargs):
        super().__init__(channels=['full'], *args, **kwargs)

    def on_open(self):
        self.msg_counter = 0
        self.messages = []

    def on_message(self, msg):
        if self.msg_counter == Scraper.CACHE_SIZE:
            self.msg_counter = 0
            # TODO: parse things
        else:
            self.messages.append(msg)
            # Using a counter is probably faster than using the list's length
            self.msg_counter += 1

    def on_close(self):
        pass
        # TODO: parse the remaining data in the list, then empty it
