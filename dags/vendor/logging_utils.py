
from logging import Logger, getLogger, Formatter, StreamHandler
from logging.handlers import RotatingFileHandler
from sys import stdout
from time import time
from typing import Callable
from vendor.config.configuration import Configuration

class WithLogger(type):

    _instances = {}

    def __new__(cls, classname, superclasses, attributedict):
        cls._instances.setdefault(
            classname, logger(f"{classname}.class"))
        logger_instance = cls._instances.get(classname)
        attributedict.update({
            "_logger": logger_instance,
            "info": WithLogger.info,
            "warn": WithLogger.warn,
            "debug": WithLogger.debug,
            "error": WithLogger.error,
            "panic": WithLogger.panic,
            "getLogger": WithLogger.getLogger
        })
        return super().__new__(cls, classname, superclasses, attributedict)

    @staticmethod
    def info(self, msg: str):
        self._logger.info(msg)

    @staticmethod
    def warn(self, msg: str):
        self._logger.warning(msg)

    @staticmethod
    def debug(self, msg: str):
        self._logger.debug(msg)

    @staticmethod
    def error(self, msg: str, ex: Exception = None):
        self._logger.error(msg, exc_info=ex, stack_info=True)

    @staticmethod
    def panic(self, msg: str, ex: Exception = None):
        self._logger.critical(msg, exc_info=ex, stack_info=True)

    @staticmethod
    def getLogger(self) -> Logger:
        return self._logger

def time_and_log(method: Callable):
    def wrapper(*args, **kwargs):
        current_ts = time()
        result = method(*args, **kwargs)
        tooks = time() - current_ts
        log = logger(method.__name__)
        log.info(f"Method invokation {method.__name__} tooks {tooks} seconds")
        return result
    return wrapper

default_log_handler = None
default_log_file_handler = None
default_log_level = 20

def logger(name: str) -> Logger:
    logger = getLogger(name)
    logger.setLevel(default_log_level)

    if not default_log_handler:
        _initialize_logging()

    if default_log_handler:
        logger.addHandler(default_log_handler)
    if default_log_file_handler:
        logger.addHandler(default_log_file_handler)
    return logger

def _initialize_logging():
    global default_log_level 
    default_log_formatter: Formatter = Formatter(
        fmt="%(asctime)s: [%(levelname)s] [%(processName)-10s] [%(threadName)s] [%(name)s]\t-\t%(message)s")
    global default_log_handler
    default_log_handler = StreamHandler(stream=stdout)
    default_log_handler.setFormatter(default_log_formatter)
