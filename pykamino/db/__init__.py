import datetime
from enum import Enum
from functools import partial

from peewee import (BigIntegerField, CharField, DateTimeField, DecimalField,
                    ForeignKeyField, Model, MySQLDatabase, PostgresqlDatabase,
                    Proxy, SqliteDatabase, UUIDField)

# We want the database to be dinamically defined, so that we
# can support different DBMSs. In order to do that, we first declare a placeholder.
database = Proxy()


class Dbms(Enum):
    MYSQL = 'mysql'
    POSTGRES = 'postgres'
    SQLITE = 'sqlite'


def db_factory(dbms: Dbms, user, psw, host, port, db_name):
    """
    Set up the database connection with the given parameters.
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
        db = SqliteDatabase(**args)
    database.initialize(db)
    database.create_tables(BaseModel.__subclasses__())
    return database


CurrencyField = partial(DecimalField, max_digits=18, decimal_places=8)


class BaseModel(Model):
    class Meta:
        database = database
        legacy_table_names = False


class Trade(BaseModel):
    """
    Represents the table of trades.

    NOTE: A trade is a match in price of two orders:
    a "buy" one and a "sell" one.
    """
    side = CharField()
    size = CurrencyField()
    product = CharField()
    price = CurrencyField()
    time = DateTimeField()

    class Meta:
        schema = 'data'

# NOTE: side and product_id could be foreign keys of a table
# containing the actual values.


class Order(BaseModel):
    id = UUIDField(primary_key=True)
    side = CharField()
    product = CharField()

    class Meta:
        schema = 'data'


class OrderTimeline(BaseModel):
    remaining_size = CurrencyField()
    price = CurrencyField()
    time = DateTimeField(default=datetime.datetime.now)
    order = ForeignKeyField(Order)
    reason = CharField(null=True)

    class Meta:
        schema = 'data'
