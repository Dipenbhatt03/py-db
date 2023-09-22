from logging.config import dictConfig

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
