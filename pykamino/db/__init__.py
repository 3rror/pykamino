from enum import Enum
from peewee import BigIntegerField, CharField, DateTimeField, DecimalField
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


class BaseModel(Model):
    class Meta:
        database = database


class Trade(BaseModel):
    """
    Represents the table of trades.

    NOTE: A trade is a match in price of two orders:
    a "buy" one and a "sell" one.
    """
    trade_id = BigIntegerField(primary_key=True)
    side = CharField()
    size = CharField()
    product_id = CharField()
    price = CharField()
    time = DateTimeField()

    class Meta:
        schema = 'data'

# NOTE: side and product_id could be foreign keys of a table
# containing the actual values.

