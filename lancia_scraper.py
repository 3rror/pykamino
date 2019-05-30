#! /bin/env python3

from pykamino._cli.shared_utils import init_db
from pykamino.scraper.websocket import Client
from pykamino.scraper.websocket import Client


def create_client():
    return Client(200)


init_db()
scraper = Client(products=['BTC-USD', 'ETH-USD'])
scraper.start()