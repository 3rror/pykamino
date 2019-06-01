import asyncio

from pykamino._cli.config import config as cfg
from pykamino._cli.shared_utils import init_db
from pykamino.scraper.websocket import Client

products = cfg['global']['products']


def run(*args, **kwargs):
    loop = asyncio.get_event_loop()
    client = Client(products=products, buffer_len=kwargs.get('buffer'))
    task = loop.create_task(client.coro())
    try:
        init_db()
        loop.run_forever()
    except KeyboardInterrupt:
        pass
    finally:
        task.cancel()
        loop.stop()
