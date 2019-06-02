from pykamino._cli.config import config as cfg
from pykamino.db import Dbms, db_factory
from os import environ, getenv


def init_db():
    conf = cfg['scraper']['database']
    if 'ON_DOCKER' in environ:
        db_factory(
            dbms=Dbms('postgres'),
            db_name=getenv('POSTGRES_DB'),
            user=getenv('POSTGRES_USER'),
            psw=getenv('POSTGRES_PASSWORD'),
            host='db',
            port='5432')
    else:
        db_factory(
            Dbms(conf['dbms']),
            conf['db_name'],
            user=conf['user'],
            psw=conf['password'],
            host=conf['hostname'],
            port=conf['port'])
