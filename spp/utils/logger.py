import logging
import sys
from logging.handlers import TimedRotatingFileHandler
from spp.utils.config import Configuration
import os

config = Configuration()
#FORMATTER = logging.Formatter("%(asctime)s — %(name)s — %(levelname)s — %(message)s")
#FORMATTER = logging.Formatter('%(asctime)s <%(name)s> <%(thread)d> [%(levelname)s] %(message)s') #, datefmt='%Y-%m-%d %H:%M:%S.%f')
FORMATTER = logging.Formatter('[%(levelname)s] %(message)s') #, datefmt='%Y-%m-%d %H:%M:%S.%f')
LOG_DIR = config.IMS_CONFIG["logging"]["log_directory"]


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


def get_logger(logger_name, log_filename=None):
    logger = logging.getLogger(logger_name)
# MTH: added to stop adding duplicate handlers
    if not len(logger.handlers):
        logger.setLevel(logging.DEBUG)  # better to have too much log than not enough
        logger.addHandler(get_console_handler())
        logger.addHandler(get_file_handler(log_filename))
        logger.propagate = False
    return logger
