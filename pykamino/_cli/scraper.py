import sys

import appdirs
import service

from pykamino._cli.config import config as cfg
from pykamino._cli.shared_utils import init_db
from pykamino.scraper.websocket import Client


class Service(service.Service):
    """
    A background process that downloads data, parse it, and store it in a
    database.
    """

    def __init__(self, *args, **kwargs):
        super().__init__('cbpro_service',
                         pid_dir=appdirs.user_cache_dir('pykamino'),
                         *args, **kwargs)
        self.scraper = Client(None, products=cfg['global']['products'])

    def run(self):
        self.scraper.start()
        self.wait_for_sigterm()

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
