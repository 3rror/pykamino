"""
Adapt Coinbase Pro's data structure to our database Models
"""

from pykamino.db import Order, OrderTimeline, Trade


def book_snapshot_to_orders(snap, product):
    for key in (k for k in snap if k in ['bids', 'asks']):
        for item in snap[key]:
            # snap[0]: price; snap[1]: size; snap[2]: uuid
            # Remove the leading 's' (for plural) from each 'side'
            order = Order(id=item[2], side=key[:-1], product=product)
            timeline = OrderTimeline(price=item[0], remaining_size=item[1], order=order)
            yield (order, timeline)


def msg_to_order(msg) -> (Order, OrderTimeline):
    """
    Convert a Coinbase message into an `OrderTimeline` instance, and the related `Order` instance.

    If the message is related to a market order, meaning the order has never been
    on the book, both variables will be `None`.

    Returns:
        order: an Order instance if available, else None
        timeline: an OrderTimeline instance if message is not a market order, else None
    Raises:
        ValueError: if message type is not 'change', 'done' or 'open'.
    """
    if msg['type'] not in ['change', 'done', 'open']:
        raise ValueError("Message type is not 'open', 'change', or 'done'")
    order = None
    if msg['type'] == 'open':
        order = Order(id=msg['order_id'], side=msg['side'], product=msg['product_id'])
    try:
        timeline = OrderTimeline(remaining_size=msg['remaining_size'],
                                price=msg['price'],
                                time=msg['time'],
                                order=order if order is not None else msg['order_id'],
                                reason=msg.get('reason', None))
    except KeyError:
        # from Coinbase: market orders will not have a remaining_size
        # or price field as they are never on the open order book at a given price.
        timeline = None
    return order, timeline


def msg_to_trade(msg) -> Trade:
    """
    Convert a Coinbase message into an `Trade` instance.

    Returns:
        trade: a Trade instance.
    Raises:
        ValueError: if message type is not 'match'.
    """
    if msg['type'] != 'match':
        raise ValueError("Message type is not 'match'")
    trade = Trade(side=msg['side'],
                  size=msg['size'],
                  product=msg['product_id'],
                  price=msg['price'],
                  time=msg['time'])
    return trade
