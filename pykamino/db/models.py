from peewee import Model
from peewee import CharField, DateTimeField, DecimalField, IntegerField


class Trade(Model):
    trade_id    = IntegerField(primary_key=True)
    side        = CharField()
    size        = CharField()
    product_id  = CharField()
    price       = CharField()
    time        = DateTimeField()
