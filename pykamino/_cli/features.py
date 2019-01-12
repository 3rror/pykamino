import multiprocessing

from pykamino.features import exporter, trades


def export_trades(*args, **kwargs):
    feats = trades.extract(
        kwargs['start_dt'], kwargs['end_dt'], kwargs['resolution'], kwargs['product'])
    exporter.features_to_csv(feats, kwargs['path'])
