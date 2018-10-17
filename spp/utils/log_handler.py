import logging
import sys
from logging.handlers import TimedRotatingFileHandler
from spp.utils.config import Configuration
import os

config = Configuration()
#FORMATTER = logging.Formatter("%(asctime)s — %(name)s — %(levelname)s — %(message)s")
FORMATTER = logging.Formatter('%(asctime)s <%(name)s> <%(thread)d> [%(levelname)s] %(message)s') #, datefmt='%Y-%m-%d %H:%M:%S.%f')
#FORMATTER = logging.Formatter('[%(levelname)s] %(message)s') #, datefmt='%Y-%m-%d %H:%M:%S.%f')
LOG_DIR = config.DATA_CONNECTOR["logging"]["log_directory"]
LOG_LEVEL = config.DATA_CONNECTOR["logging"]["log_level"]


def get_console_handler():
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(FORMATTER)
    return console_handler


def get_file_handler(log_filename):
    if not os.path.exists(LOG_DIR):
        os.makedirs(LOG_DIR)
    file_handler = TimedRotatingFileHandler(LOG_DIR + log_filename, when='midnight')
    file_handler.setFormatter(FORMATTER)
    return file_handler


def get_logger(logger_name, log_filename, log_level=None):
    logger = logging.getLogger(logger_name)
    # MTH: added to stop adding duplicate handlers
    if not len(logger.handlers):

        # Set Log Level based on the way it was passed
        final_log_level = "INFO"
        if log_level is not None:
            final_log_level = log_level
        elif LOG_LEVEL is not None:
            final_log_level = LOG_LEVEL
        logger.setLevel(final_log_level)

        logger.addHandler(get_console_handler())
        logger.addHandler(get_file_handler(log_filename))
        logger.propagate = False
    return logger
