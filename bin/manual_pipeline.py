from redis import StrictRedis
from spp.core.settings import settings
from microquake.core import read
from io import BytesIO
from loguru import logger
from spp.pipeline.manual_pipeline import manual_pipeline
import msgpack

logger.info('initializing connection to Redis')
redis_settings = settings.get('redis_db')
message_queue = settings.get(
    'processing_flow').manual.message_queue

redis = StrictRedis(**redis_settings)

logger.info('initialization successful')

while 1:
    logger.info('waiting for message on channel %s' % message_queue)
    message_queue, message = redis.blpop(message_queue)
    logger.info('message received')

    tmp = msgpack.loads(message)
    data = {}
    for key in tmp.keys():
        data[key.decode('utf-8')] = tmp[key]

    manual_pipeline(**data)


# def test_manual_pipeline():
#     logger.info('loading mseed data')
#     # mseed_bytes = requests.get("https://permanentdbfilesstorage.blob.core"
#     #                            ".windows.net/permanentdbfilesblob/events/2019-06"
#     #                            "-09T033053.080047Z.mseed").content
#     #
#     # with open('test_data.mseed', 'wb') as fout:
#     #     fout.write(mseed_bytes)
#
#     with open('test_data.mseed', 'rb') as fin:
#         mseed_bytes = fin.read()
#
#     logger.info('done loading mseed data')
#
#     fixed_length_wf = read(BytesIO(mseed_bytes), format='mseed')
#
#     logger.info('loading catalogue data')
#     # catalog_bytes = requests.get(
#     #     "https://permanentdbfilesstorage.blob.core.windows.net"
#     #     "/permanentdbfilesblob/events/2019-06-09T033053.047217Z.xml").content
#     #
#     # with open('test_data.xml', 'wb') as fout:
#     #     fout.write(catalog_bytes)
#
#     with open('test_data.xml', 'rb') as fin:
#         catalog_bytes = fin.read()
#
#     logger.info('done loading catalogue data')
#
#     cat = read_events(BytesIO(catalog_bytes), format='quakeml')
#
#     bytes_out = BytesIO()
#     fixed_length_wf.write(bytes_out, format='mseed')
#
#     logger.info('sending request to the ray tracer on channel %s'
#                 % ray_tracer_message_queue)
#
#     cat_out = {'cat': catalog_bytes, 'fixed_length': mseed_bytes}
#
#     redis.rpush(message_queue, cat_out)
#
#     # automatic_pipeline(fixed_length_wf)