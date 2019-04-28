from functools import wraps


def rounded(func=None, *, ndigits=8):
    """
    Round the result of the given function to the specified
    number of digits.
    """
    def decorate(function):
        @wraps(function)
        def round_it(*args, **kwargs):
            num = func(*args, **kwargs)
            if num is None or num == 0:
                return num
            return round(num, ndigits)
        return round_it
    if func:
        return decorate(func)
    return decorate
