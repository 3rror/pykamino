from datetime import datetime
from functools import partial
import enum
import math
import os

from playhouse import pool
import peewee

# We want the database to be dinamically defined, so that we can support
# different Dbms's. In order to do that, we first declare a placeholder.
database = peewee.DatabaseProxy()


class Dbms(enum.Enum):
    """
    An enum repesenting a set of supported DBMSs.
    """
    MYSQL = 'mysql'
    POSTGRES = 'postgres'
    SQLITE = 'sqlite'


def db_factory(dbms: Dbms, db_name, user=None, psw=None, host=None, port=None):
    """
    Set up the database connection with the given parameters and create needed
    tables and schemas.

    You must call this function before any operation on the database.
    """
    args = {'database': db_name,
            'user': user,
            'password': psw,
            'host': host,
            'port': port,
            # We don't want too many connections, but we want
            # at least two (for fast feature extraction)
            'max_connections': math.ceil(os.cpu_count() / 2) if os.cpu_count() > 2 else 2}
    if dbms == Dbms.MYSQL:
        real_db = pool.PooledMySQLDatabase(**args)
    elif dbms == Dbms.POSTGRES:
        real_db = pool.PooledPostgresqlDatabase(**args)
    elif dbms == Dbms.SQLITE:
        real_db = pool.PooledSqliteDatabase(db_name)
    database.initialize(real_db)
    database.create_tables(BaseModel.__subclasses__())
    database.manual_close()
    return real_db


CurrencyField = partial(peewee.DecimalField, max_digits=18, decimal_places=8)
CurrencyField.__doc__ = """A model corresponding to a fixed-point number with
8 decimal places and 10 digits for the integer part."""


class EnumField(peewee.SmallIntegerField):
    """
    A `peewee.SmallIntegerField` that maps an integer number to a string, and vice-versa.
    """

    def __init__(self, keys, *args, **kwargs):
        super().__init__(null=False, *args, **kwargs)
        self.enum = enum.Enum('InnerEnum', ' '.join(keys))

    # Overridden
    def db_value(self, value):
        return self.enum[value].value

    # Overridden
    def python_value(self, value):
        return self.enum(value).name


CryptoField = partial(EnumField, keys=('BTC-USD', 'ETH-USD'))
CryptoField.__doc__ = """An EnumField for "BTC-USD" and "ETH-USD"."""


class BaseModel(peewee.Model):
    """
    A base model for all the ORM models used in pykamino.
    You should extend this class if you want to define models
    using the same `pykamino` database.
    """
    class Meta:
        database = database
        legacy_table_names = False


class Trade(BaseModel):
    """
    Trade Represents the table of trades.

    Note:
        A trade is a match in price of two orders: a "buy" one and a "sell" one.
    """
    side = EnumField(keys=('sell', 'buy'))
    amount = CurrencyField()
    product = CryptoField()
    price = CurrencyField()
    time = peewee.DateTimeField()

    class Meta:
        table_name = 'trades'
        indexes = ((('product', 'time'), False),)


class OrderState(BaseModel):
    """
    OrderState represents the table of order states, i.e. the entries
    in the order book.
    """
    order_id = peewee.UUIDField()
    product = CryptoField()
    side = EnumField(keys=('ask', 'bid'))
    price = CurrencyField()
    amount = CurrencyField()
    starting_at = peewee.DateTimeField(default=datetime.utcnow)
    ending_at = peewee.DateTimeField(null=True)

    class Meta:
        primary_key = peewee.CompositeKey('order_id', 'starting_at')
        table_name = 'order_states'
        indexes = ((('product', 'ending_at', 'starting_at'), False),)
        constraints = [peewee.Check('starting_at < ending_at')]
