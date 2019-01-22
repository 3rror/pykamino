from pykamino._cli.config import config as cfg
from pykamino._cli.shared_utils import init_db
from pykamino.features import exporter, orders, trades


def compute(*args, **kwargs):
    category = kwargs['category']
    params = {'start_dt': kwargs['start'],
              'end_dt': kwargs['end'],
              'res': kwargs['resolution'],
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
