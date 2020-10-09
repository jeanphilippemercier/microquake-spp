from loguru import logger
import sys

from spp.core.settings import settings

logging_level = settings.get('debug_level')
logger.add(sys.stderr, level=logging_level)


