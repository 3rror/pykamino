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


class BaseModel(Model):
    class Meta:
        database = database


class Trade(BaseModel):
    trade_id = BigIntegerField(primary_key=True)
    side = CharField()
    size = CharField()
    product_id = CharField()
    price = CharField()
    time = DateTimeField()

    class Meta:
        schema = 'data'
