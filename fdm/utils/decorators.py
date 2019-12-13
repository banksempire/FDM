import functools
from threading import Thread
from time import sleep


def retry(max_retry=10, wait=1):
    def decorator(func):
        @functools.wraps(func)
        def warpper(*args, **kwargs):
            for _ in range(max_retry):
                try:
                    return func(*args, **kwargs)
                except Exception:
                    print('Function {0} args {1} kwargs {2}, retry attempt: {3}'.format(
                        func.__name__, args, kwargs, _+1))
                    sleep(wait)
        return warpper
    return decorator


def timeout(seconds_before_timeout):
    def deco(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            res = [Exception('function [%s] timeout [%s seconds] exceeded!' % (
                func.__name__, seconds_before_timeout))]

            def newFunc():
                try:
                    res[0] = func(*args, **kwargs)
                except Exception as e:
                    res[0] = e
            t = Thread(target=newFunc)
            t.daemon = True
            try:
                t.start()
                t.join(seconds_before_timeout)
            except Exception as e:
                print('error starting thread')
                raise e
            ret = res[0]
            if isinstance(ret, BaseException):
                raise ret
            return ret
        return wrapper
    return deco
