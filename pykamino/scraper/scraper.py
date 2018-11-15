import cbpro


class Scraper:
    def __init__(self, products, mongo_collection):
        self.websocket = PykaminoWebsocket(
            products=products, mongo_collection=mongo_collection)

    def start(self):
        self.websocket.start()

    def stop(self):
        self.websocket.stop()


class PykaminoWebsocket(cbpro.WebsocketClient):
    CACHE_SIZE = 1000

    def on_open(self):
        self.msg_counter = 0

    def on_message(self):
        if self.msg_counter == PykaminoWebsocket.CACHE_SIZE:
            self.msg_counter = 0
            # TODO: parse things in mongodb
        else:
            self.msg_counter += 1

    def on_close(self):
        pass
        # TODO: parse the remaining data in mongodb, then empty it
