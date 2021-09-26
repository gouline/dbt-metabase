import sys
import logging
from logging.handlers import RotatingFileHandler
from functools import lru_cache
from pathlib import Path

from rich.logging import RichHandler


# Config Log Files
LOG_FILE_FORMAT = logging.Formatter(
    "%(asctime)s — %(name)s — %(levelname)s — %(message)s"
)
LOG_PATH = Path.home().absolute() / ".dbt-metabase" / "logs"

# Config Console Log Level
LOGGING_LEVEL = logging.INFO


def rotating_log_handler(
    logger_name: str,
) -> RotatingFileHandler:
    """This handler writes warning and higher level outputs to logs in a home .dbt-metabase directory rotating them as needed"""
    LOG_PATH.mkdir(parents=True, exist_ok=True)
    handler = RotatingFileHandler(
        str(LOG_PATH / f"{logger_name}.log"), maxBytes=int(1e6), backupCount=3
    )
    handler.setFormatter(LOG_FILE_FORMAT)
    handler.setLevel(logging.WARNING)
    return handler


def console_log_handler(
    level: str,
) -> logging.StreamHandler:
    """This handler routes logging output to the console but we are using rich instead"""
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(LOG_FILE_FORMAT)
    handler.setLevel(getattr(logging, level.upper()))
    return handler


@lru_cache
def logger(
    logger_name: str = "dbtmetabase",
) -> logging.Logger:
    """Builds and caches loggers

    Args:
        logger_name (str, optional): Logger name, also used for output log file name in `./logs` directory. Most often used with __file__ in a script importing the logging utils. Defaults to "source_fleet_management".
        level (str, optional): Logging level, this is explicitly passed to console handler which effects what level of log messages make it to the console. Defaults to "INFO".

    Returns:
        logging.Logger: Prepared logger with rotating logs and console streaming. You can use the logger directly from this function ie. `utils.logging.logger(__file__).debug("My message")` because the constructed logger is cached or you can assign an object locally `script_logger = utils.logging.logger(__file__)`
    """
    _logger = logging.getLogger(logger_name)
    _logger.setLevel(LOGGING_LEVEL)
    _logger.addHandler(rotating_log_handler(logger_name))
    _logger.addHandler(
        RichHandler(
            level=LOGGING_LEVEL,
            rich_tracebacks=True,
            markup=True,
            show_time=False,
        )
    )
    _logger.propagate = False
    return _logger
