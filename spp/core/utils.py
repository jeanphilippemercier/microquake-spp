from time import time


def timing(f):
    """
    Decorator function that measures the time spent in a function
    """
    def wrap(*args):
        time1 = time()
        ret = f(*args)
        time2 = time()
        print('{:s} function took {:.3f} ms'.format(f.__name__, (time2-time1)*1000.0))

        return ret

    return wrap
