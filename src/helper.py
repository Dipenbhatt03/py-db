import logging
from time import time

logger = logging.getLogger(__name__)


def timing(fun, *args, **kwargs):
    def wrapper(*args, **kwargs):
        t = time()
        fun(*args, **kwargs)
        logger.info(f"Executed in {time() - t} seconds")

    return wrapper
