from enum import Enum
from peewee import BigIntegerField, CharField, DateTimeField, DecimalField, ForeignKeyField, UUIDField
from peewee import Model
from peewee import MySQLDatabase, PostgresqlDatabase, Proxy, SqliteDatabase

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
    global database
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
    database.create_tables([kls for kls in BaseModel.__subclasses__()])
    return database


class CurrencyField(DecimalField):
    def __init__(self, auto_round=False, rounding=None, *args, **kwargs):
        super().__init__(max_digits=17, decimal_places=8, auto_round=False,
                         rounding=None, *args, **kwargs)


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
    trade_id = BigIntegerField(primary_key=True)
    side = CharField()
    size = CurrencyField()
    product_id = CharField()
    price = CurrencyField()
    time = DateTimeField()

    class Meta:
        schema = 'data'

# NOTE: side and product_id could be foreign keys of a table
# containing the actual values.


class Order(BaseModel):
    order_id = UUIDField(primary_key=True)
    side = CharField()
    product_id = CharField()

    class Meta:
        schema = 'data'


class OrderTimeline(BaseModel):
    remaining_size = CurrencyField()
    price = CurrencyField()
    time = DateTimeField()
    order_id = ForeignKeyField(Order)

    class Meta:
        schema = 'data'
