from pykamino._cli.config import config as cfg
from pykamino._cli.shared_utils import init_db
from pykamino.features import exporter, orders, trades
from pykamino.features import TimeWindow


def compute(*args, **kwargs):
    category = kwargs['category']
    params = {'start': kwargs['start'],
              'end': kwargs['end'],
              'res': kwargs['resolution'],
              'products': cfg['global']['products'],
              'stride': kwargs['stride'],
              'path': kwargs['path']}
    init_db()
    if category == 'all':
        export_orders(**params)
        export_trades(**params)
    elif category == 'orders':
        export_orders(**params)
    else:
        export_trades(**params)


def export_trades(start, end, res, stride, products, path):
    interval = TimeWindow(start, end)
    feats = trades.extract(interval, res, stride, products)
    exporter.features_to_csv(feats, path + '/trades.csv')


def export_orders(start, end, res, products, path, **kwargs):
    # feats = orders.extract(start, end, res, products)
    interval = TimeWindow(start, end)
    feats = orders.extract(interval, res, products)
    exporter.features_to_csv(feats, path + '/orders.csv')
