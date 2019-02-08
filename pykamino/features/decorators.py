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
            return round(num, ndigits)
        return round_it
    if func:
        return decorate(func)
    return decorate


def feature(func):
    "Mark function or method as a feature we want to extract."
    def tag_it(f):
        f.is_feature = True
        return f
    return tag_it
