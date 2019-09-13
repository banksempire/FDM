from time import sleep


def retry(max_retry=10, wait=1):
    def decorator(func):
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
