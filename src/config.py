import os
import sys
from logging.config import dictConfig

DATABASE_FILE_NAME = "../dipen.db"
if "unittest" in sys.modules.keys():
    DATABASE_FILE_NAME = f"test.db"
    if os.path.exists(DATABASE_FILE_NAME):
        os.remove(DATABASE_FILE_NAME)


def open_db_connection():
    if not os.path.exists(DATABASE_FILE_NAME):
        # Create the file if it doesn't exist
        with open(DATABASE_FILE_NAME, "wb"):
            pass
    return open(DATABASE_FILE_NAME, "r+b")


DATABASE_FD = open_db_connection()

config = {
    "version": 1,
    "disable_existing_loggers": False,
    "formatters": {
        "verbose": {
            "format": "[%(asctime)s] [%(levelname)s] [%(name)s] [%(filename)s:%(lineno)d] - %(message)s",
            "datefmt": "%Y-%m-%d %H:%M:%S %z",
        }
    },
    "handlers": {
        "console": {"level": "DEBUG", "class": "logging.StreamHandler", "formatter": "verbose"},
        "file": {"level": "INFO", "class": "logging.FileHandler", "filename": "access.log", "formatter": "verbose"},
    },
    "root": {"level": "INFO", "handlers": ["console", "file"]},
}
dictConfig(config)
