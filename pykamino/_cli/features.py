from pykamino._cli.config import config as cfg
from pykamino._cli.shared_utils import init_db
from pykamino.features import exporter, orders, trades
from datetime import timedelta


def compute(*args, **kwargs):
    category = kwargs['category']
    params = {'start_dt': kwargs['start'],
              'end_dt': kwargs['end'],
              'res': _convert_to_timedelta(kwargs['resolution']),
              'products': cfg['global']['products'],
              'path': kwargs['path']}
    init_db()
    if category == 'all':
        export_orders(**params)
        export_trades(**params)
    elif category == 'orders':
        export_orders(**params)
    else:
        export_trades(**params)


def export_trades(start_dt, end_dt, res, products, path):
    feats = trades.extract(start_dt, end_dt, res, products)
    exporter.features_to_csv(feats, path + '/trades.csv')


def export_orders(start_dt, end_dt, res, products, path):
    feats = orders.extract(start_dt, end_dt, res, products)
    exporter.features_to_csv(feats, path + '/orders.csv')


def _convert_to_timedelta(time_val):
    """
    Given a *time_val* (string) such as '5d', returns a timedelta object
    representing the given value (e.g. timedelta(days=5)).  Accepts the
    following '<num><char>' formats:

    =========   ======= ===================
    Character   Meaning Example
    =========   ======= ===================
    s           Seconds '60s' -> 60 Seconds
    m           Minutes '5m'  -> 5 Minutes
    h           Hours   '24h' -> 24 Hours
    d           Days    '7d'  -> 7 Days
    =========   ======= ===================

    Examples::

        >>> convert_to_timedelta('7d')
        datetime.timedelta(7)
        >>> convert_to_timedelta('24h')
        datetime.timedelta(1)
        >>> convert_to_timedelta('60m')
        datetime.timedelta(0, 3600)
        >>> convert_to_timedelta('120s')
        datetime.timedelta(0, 120)
    """
    num = int(time_val[:-1])
    if time_val.endswith('s'):
        return timedelta(seconds=num)
    elif time_val.endswith('m'):
        return timedelta(minutes=num)
    elif time_val.endswith('h'):
        return timedelta(hours=num)
    elif time_val.endswith('d'):
        return timedelta(days=num)

