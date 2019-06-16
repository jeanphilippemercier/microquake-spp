from redis import StrictRedis
from spp.core.settings import settings
from microquake.core import read
from io import BytesIO
from loguru import logger
from spp.pipeline.automatic_pipeline import automatic_pipeline

logger.info('initializing connection to Redis')
redis_settings = settings.get('redis_db')
message_queue = settings.get(
    'processing_flow').automatic.message_queue

redis = StrictRedis(**redis_settings)

logger.info('initialization successful')

while 1:
    logger.info('waiting for message on channel %s' % message_queue)
    message_queue, message = redis.blpop(message_queue)
    logger.info('message received')

    fixed_length = read(BytesIO(message), format='mseed')
    automatic_pipeline(fixed_length)
