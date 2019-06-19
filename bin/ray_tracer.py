from redis import StrictRedis
from spp.core.settings import settings
from microquake.core import read_events
from io import BytesIO
from loguru import logger
from spp.pipeline import ray_tracer

def test_ray_tracer():

    logger.info('loading catalogue data')
    # catalog_bytes = requests.get(
    #     "https://permanentdbfilesstorage.blob.core.windows.net"
    #     "/permanentdbfilesblob/events/2019-06-09T033053.047217Z.xml").content
    #
    # with open('test_data.xml', 'wb') as fout:
    #     fout.write(catalog_bytes)

    with open('test_data.xml', 'rb') as fin:
        catalog_bytes = fin.read()

    logger.info('done loading catalogue data')

    redis.rpush(message_queue, catalog_bytes)


logger.info('initializing connection to Redis')
redis_settings = settings.get('redis_db')
message_queue = settings.get('processing_flow').ray_tracing.message_queue

redis = StrictRedis(**redis_settings)

logger.info('initialization successful')

while 1:
    logger.info('waiting for message on channel %s' % message_queue)
    message_queue, message = redis.blpop(message_queue)
    logger.info('message received')

    cat = read_events(BytesIO(message), format='QUAKEML')
    logger.info('calculating rays for event %s'
                 % cat[0].origins[-1].time)

    ray_tracer_processor = ray_tracer.Processor()
    ray_tracer_processor.initializer()
    ray_tracer_processor.process(cat=cat)

    logger.info('done calculating rays for event %s'
                 % cat[0].origins[-1].time)
