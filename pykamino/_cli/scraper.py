from pykamino._config import config as cfg
from pykamino.db import db_factory, Dbms
from pykamino.scraper.websocket import Client
import appdirs
import service
import sys


class Service(service.Service):
    """
    A background process that downloads data, parse it, and store it in a database.
    """

    def __init__(self, *args, **kwargs):
        super().__init__('cbpro_service',
                         pid_dir=appdirs.user_cache_dir('pykamino'), *args, **kwargs)
        self.scraper = Client(None, products=cfg['global']['products'])

    def run(self):
        self.scraper.start()
        self.wait_for_sigterm()

    def stop(self, block=False):
        super().stop(block=block)


service = Service()


### CLI commands ###
def run(*args, **kwargs):
    if service.is_running():
        print('Service already running', file=sys.stderr)
    else:
        service.scraper.buffer_length = kwargs['buffer']
        conf = cfg['scraper']['database']
        db_factory(Dbms(conf['dbms']), conf['user'], conf['password'],
                   conf['hostname'], conf['port'], conf['db_name'])
        service.start()


def stop(*args, **kwargs):
    service.stop(block=True)
