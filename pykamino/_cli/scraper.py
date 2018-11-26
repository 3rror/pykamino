from pykamino._config import config as cfg
from pykamino.scraper import Scraper
import appdirs
import service
import sys


class Service(service.Service):
    def __init__(self, *args, **kwargs):
        super().__init__('cbpro_service',
                         pid_dir=appdirs.user_cache_dir('pykamino'), *args, **kwargs)
        self.scraper = Scraper(products=cfg['global']['products'])

    def run(self):
        self.scraper.start()
        self.wait_for_sigterm()

    def stop(self, block=False):
        super().stop(block=block)


service = Service()


def run(*args, **kwargs):
    if service.is_running():
        print('Service already running', file=sys.stderr)
    else:
        service.start()


def stop(*args, **kwargs):
    service.stop()
