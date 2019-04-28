from pykamino._cli.config import config as cfg
from pykamino.db import Dbms, db_factory


def init_db():
    conf = cfg['scraper']['database']
    db_factory(
        Dbms(conf['dbms']),
        conf['db_name'],
        user=conf['user'],
        psw=conf['password'],
        host=conf['hostname'],
        port=conf['port'])
