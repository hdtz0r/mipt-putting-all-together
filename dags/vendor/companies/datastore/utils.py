
from time import sleep, time
from typing import Callable


class Backoff:

    _delay: float
    _timestamp: int

    def __init__(self, delay: float) -> None:
        self._delay = delay
        self._timestamp = time()

    def set(self):
        self._timestamp = time()

    def is_expired(self) -> bool:
        return time() > self._timestamp + self._delay
    
class ExponentialBackoff(Backoff):

    pass


def retry(error: Exception, backoff=Backoff(1)):
    def _retry_wrapper(f: Callable):
        def _enclosed_function(*args: any, **kwargs: any) -> any:          
            while True:
                if backoff.is_expired():
                    try:
                        return f(*args, **kwargs)
                    except Exception as ex:
                        if isinstance(ex, error):
                            backoff.set()
                            sleep(backoff._delay)
                        else:
                            raise
                else:
                    sleep(0.01)
        return _enclosed_function
    return _retry_wrapper
