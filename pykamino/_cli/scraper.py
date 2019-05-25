import sys
from time import sleep

import appdirs
import service

from pykamino._cli.config import config as cfg
from pykamino._cli.shared_utils import init_db
from pykamino.scraper.websocket import Client
from requests.exceptions import ConnectionError
from requests.packages.urllib3.exceptions import NewConnectionError, MaxRetryError


def create_client():
    return Client(None, products=cfg['global']['products'])

class Service(service.Service):
    """
    A background process that downloads data, parse it, and store it in a
    database.
    """

    def __init__(self, *args, **kwargs):
        super().__init__('cbpro_service',
                         pid_dir=appdirs.user_cache_dir('pykamino'),
                         *args, **kwargs)
        self.scraper = create_client()

    def run(self):
        self.scraper.start()
        while not self.got_sigterm():
            # Unfortunately Coinbase Pro has the bad habit of
            # dropping the WS connection randomly. Let's recreate
            # it if that happens.
            sleep(5)
            if not scraper.is_running():
                print('Sto chiudendo')
                scraper.stop()
                print('Ho chiuso')
                scraper = create_client()
                while True:
                    try:
                        print('Provo a far ripartire')
                        scraper.start()
                    except (ConnectionError, NewConnectionError, MaxRetryError):
                        sleep(0.5)
                        continue
                    else:
                        print('Partito')
                        break

    def stop(self, block=False):
        super().stop(block=block)


service = Service()


# CLI commands #
def run(*args, **kwargs):
    if service.is_running():
        print('The service is already running', file=sys.stderr)
    else:
        service.scraper.buffer_length = kwargs['buffer']
        init_db()
        service.start()


def stop(*args, **kwargs):
    if not service.is_running():
        print('The service is not running', file=sys.stderr)
    else:
        service.stop(block=True)
