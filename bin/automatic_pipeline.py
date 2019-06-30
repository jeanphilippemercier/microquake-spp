from redis import Redis
from spp.core.settings import settings
from loguru import logger
from spp.pipeline.automatic_pipeline import automatic_pipeline
# import msgpack
from microquake.io import msgpack


def test_automatic_pipeline():
    import requests
    from microquake.core import read
    from microquake.core import read_events
    from io import BytesIO
    from spp.core.settings import settings

    logger.info('initializing connection to Redis')
    redis_settings = settings.get('redis_db')
    message_queue = settings.get(
        'processing_flow').automatic.message_queue

    redis = Redis(**redis_settings)

    logger.info('loading mseed data')
    # mseed_bytes = requests.get("https://permanentdbfilesstorage.blob.core"
    #                            ".windows.net/permanentdbfilesblob/events/2019-06"
    #                            "-09T033053.080047Z.mseed").content

    # with open('test_data.mseed', 'wb') as fout:
    #     fout.write(mseed_bytes)
    #
    with open('test_data.mseed', 'rb') as fin:
        mseed_bytes = fin.read()

    context_bytes = requests.get("https://permanentdbfilesstorage.blob.core"
                               ".windows.net/permanentdbfilesblob/events/2019-06"
                               "-09T033053.080047Z.context_mseed").content

    logger.info('done loading mseed data')

    fixed_length_wf = read(BytesIO(mseed_bytes), format='mseed')

    logger.info('loading catalogue data')
    # catalog_bytes = requests.get(
    #     "https://permanentdbfilesstorage.blob.core.windows.net"
    #     "/permanentdbfilesblob/events/2019-06-09T033053.047217Z.xml").content

    # with open('test_data.xml', 'wb') as fout:
    #     fout.write(catalog_bytes)

    with open('test_data.xml', 'rb') as fin:
        catalog_bytes = fin.read()

    logger.info('done loading catalogue data')

    cat = read_events(BytesIO(catalog_bytes), format='quakeml')
    context = read(BytesIO(context_bytes), format='mseed')

    dict_out = {'stream': fixed_length_wf, 'context': context,
                'cat': cat}
    msg_out = msgpack.pack(dict_out)

    bytes_out = BytesIO()
    fixed_length_wf.write(bytes_out, format='mseed')

    logger.info('sending request to the automatic pipeline on channel %s'
                % message_queue)

    redis.rpush(message_queue, msg_out)


logger.info('initializing connection to Redis')
redis_settings = settings.get('redis_db')
message_queue = settings.get(
    'processing_flow').automatic.message_queue

redis = Redis(**redis_settings)

logger.info('initialization successful')

while 1:
    logger.info('waiting for message on channel %s' % message_queue)
    message_queue, message = redis.blpop(message_queue)
    logger.info('message received')

    tmp = msgpack.unpack(message)
    data = {}
    for key in tmp.keys():
        data[key] = tmp[key]

    automatic_pipeline(**data)
