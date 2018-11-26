from pykamino._config import config as cfg
from pykamino.scraper.scraper import Scraper
import appdirs
import service
import time


class Service(service.Service):
    def __init__(self, *args, **kwargs):
        super().__init__('cbpro_service',
                         pid_dir=appdirs.user_cache_dir('pykamino'), *args, **kwargs)
        self.scraper = Scraper(products=cfg['global']['products'])

    def run(self):
        self.scraper.start()
        while not self.got_sigterm():
            time.sleep(3)

    def stop(self, block=False):
        self.scraper.close()
        super().stop(block=block)


def run(*args, **kwargs):
    Service().start()
