import asyncio

from pykamino._cli.config import config as cfg
from pykamino._cli.shared_utils import init_db
from pykamino.scraper.websocket import Client

products = cfg['global']['products']


def run(*args, **kwargs):
    try:
        init_db()
        client = Client(products=products, buffer_len=kwargs.get('buffer'))
        loop = asyncio.get_event_loop()
        task = loop.create_task(client.coro())
        loop.run_forever()
    except KeyboardInterrupt:
        pass
    finally:
        task.cancel()
        loop.stop()
