import datetime
from enum import Enum
from functools import partial

from peewee import (SQL, BigIntegerField, CharField, DateTimeField,
                    DecimalField, ForeignKeyField, Model, MySQLDatabase,
                    PostgresqlDatabase, Proxy, SqliteDatabase, UUIDField)

# We want the database to be dinamically defined, so that we
# can support different DBMSs. In order to do that, we first declare a placeholder.
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
            'port': port}
    if dbms == Dbms.MYSQL:
        db = MySQLDatabase(**args)
    elif dbms == Dbms.POSTGRES:
        db = PostgresqlDatabase(**args)
    elif dbms == Dbms.SQLITE:
        db = SqliteDatabase(db_name)
    database.initialize(db)
    database.create_tables(BaseModel.__subclasses__())
    return database


CurrencyField = partial(DecimalField, max_digits=18, decimal_places=8)
Iso8601DateTimeField = partial(DateTimeField,
                               formats=['%Y-%m-%dT%H:%M:%S.%fZ', '%Y-%m-%dT%H:%M:%S.%f',
                                        # This one is for SQLite
                                        '%Y-%m-%d %H:%M:%f'])


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
    side = CharField(4)
    amount = CurrencyField()
    product = CharField(7)
    price = CurrencyField()
    time = Iso8601DateTimeField()

    class Meta:
        indexes = ((('product', 'time'), False),)


class Order(BaseModel):
    id = UUIDField(primary_key=True)
    side = CharField(4)
    product = CharField(7)
    price = CurrencyField()
    close_time = Iso8601DateTimeField(null=True)


class OrderHistory(BaseModel):
    amount = CurrencyField()
    time = Iso8601DateTimeField(default=datetime.datetime.now)
    order = ForeignKeyField(Order, backref='history')

    class Meta:
        constraints = [SQL('UNIQUE (amount, order_id)')]
