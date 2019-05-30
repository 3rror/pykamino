from datetime import datetime
from enum import Enum
from functools import partial
from math import ceil
from os import cpu_count

import iso8601
from peewee import (CharField, Check, CompositeKey, DateTimeField,
                    DecimalField, Model, Proxy, SmallIntegerField, UUIDField)
from playhouse.pool import (PooledMySQLDatabase, PooledPostgresqlDatabase,
                            PooledSqliteDatabase)

# We want the database to be dinamically defined, so that we can support
# different Dbms's. In order to do that, we first declare a placeholder.
database = Proxy()


class Dbms(Enum):
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
            'max_connections': ceil(cpu_count() / 2) if cpu_count() > 2 else 2
            }
    if dbms == Dbms.MYSQL:
        db = PooledMySQLDatabase(**args)
    elif dbms == Dbms.POSTGRES:
        db = PooledPostgresqlDatabase(**args)
    elif dbms == Dbms.SQLITE:
        db = PooledSqliteDatabase(db_name)
    database.initialize(db)
    with database:
        database.create_tables(BaseModel.__subclasses__())
    return database


CurrencyField = partial(DecimalField, max_digits=18, decimal_places=8)


class Iso8601DateTimeField(DateTimeField):
    # This is needed for SQlite3 only
    formats = ['%Y-%m-%d %H:%M:%f']

    def adapt(self, value):
        """
        adapt overrides the original method so that it's possible to parse the ISO8601
        format. This format in fact is not parseable with the usual format strings
        but it needs specific logic to deal with implicit zeroes.
        """
        try:
            return iso8601.parse_date(value)
        except iso8601.ParseError:
            return super().adapt(value)


class EnumField(SmallIntegerField):
    def __init__(self, keys, *args, **kwargs):
        super().__init__(null=False, *args, **kwargs)
        self.enum = Enum('InnerEnum', ' '.join(keys))

    def db_value(self, value):
        return self.enum[value].value

    def python_value(self, value):
        return self.enum(value).name


class BaseModel(Model):
    class Meta:
        database = database
        legacy_table_names = False


class Trade(BaseModel):
    """
    Represents the table of trades.

    Note: A trade is a match in price of two orders:
    a "buy" one and a "sell" one.
    """
    side = EnumField(keys=('sell', 'buy'))
    amount = CurrencyField()
    product = EnumField(keys=('BTC-USD', 'ETH-USD'))
    price = CurrencyField()
    time = Iso8601DateTimeField()

    class Meta:
        table_name = 'trades'
        # Note: the ending comma is required
        indexes = ((('product', 'time'), False),)


class OrderState(BaseModel):
    order_id = UUIDField()
    product = EnumField(keys=('BTC-USD', 'ETH-USD'))
    side = EnumField(keys=('ask', 'bid'))
    price = CurrencyField()
    amount = CurrencyField()
    starting_at = Iso8601DateTimeField(default=datetime.now)
    ending_at = Iso8601DateTimeField(null=True)

    class Meta:
        primary_key = CompositeKey('order_id', 'starting_at')
        table_name = 'order_states'
        # Note: the ending comma is required
        indexes = ((('product', 'ending_at', 'starting_at'), False),)
        constraints = [Check('starting_at < ending_at')]
