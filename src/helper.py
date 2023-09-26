import logging
from time import time

from src.config import DATABASE_FD

logger = logging.getLogger(__name__)


def timing(fun, *args, **kwargs):
    def wrapper(*args, **kwargs):
        t = time()
        fun(*args, **kwargs)
        logger.info(f"Executed in {time() - t} seconds")

    return wrapper


def seek_db_fd(offset):
    if DATABASE_FD.tell() != offset:
        DATABASE_FD.seek(offset)


