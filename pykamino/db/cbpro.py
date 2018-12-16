"""
Adapt Coinbase Pro's data structure to our database Models
"""

from playhouse.shortcuts import dict_to_model
from pykamino.db import Order, OrderTimeline


def book_snapshot_to_orders(snap, product):
    orders = []
    timelines = []
    for key in (k for k in snap if k in ['bids', 'asks']):
        for item in snap[key]:
            # Remove the leading 's' (for plural) from each 'side'
            orders.append(Order(id=item[2], side=key[:-1], product=product))
            timelines.append(OrderTimeline(price=item[0],
                                           remaining_size=item[1],
                                           order=orders[-1]))
    return orders, timelines


def dict_to_orders(orders):
    """
    Return two lists: the former is a list of pure `Order` instances, the latter
    is a list of `OrderTimeline` objects.
    """
    orders_to_save = []
    timelines = []

    def dict_to_timeline(order): timelines.append(
        dict_to_model(OrderTimeline, order, ignore_unknown=True))
    for order in orders:
        if order['type'] in ['change', 'done']:
            dict_to_timeline(order)
        elif order['type'] == 'open':
            orders_to_save.append(dict_to_model(
                Order, order, ignore_unknown=True))
            dict_to_timeline(order)
    return orders_to_save, timelines
