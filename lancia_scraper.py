#! /bin/env python3

from pykamino._cli.shared_utils import init_db
from pykamino.scraper.websocket import Client
from time import sleep
from requests.packages.urllib3.exceptions import NewConnectionError


def create_client():
    return Client(200)


init_db()
scraper = create_client()
scraper.start()
while True:
    sleep(5)
    if not scraper.is_running():
        while True:
            try:
                print('Provo a far ripartire')
                scraper.start()
            except NewConnectionError:
                continue
            else:
                print('Partito')
                break
