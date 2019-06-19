from datetime import datetime
from struct import unpack
from uuid import uuid4
from loguru import logger

import numpy as np

from microquake.io.waveform import mseed_date_from_header
from pymongo import MongoClient
from redis import Redis
from spp.utils.application import Application


def extract_mseed_header(mseed_bytes):
    record = mseed_bytes
    station = unpack('5s', record[8:13])[0].strip().decode()
    channel = unpack('3s', record[15:18])[0].strip().decode()
    network = unpack('2s', record[18:20])[0].strip().decode()
    number_of_sample = unpack('>H', record[30:32])[0]
    sample_rate_factor = unpack('>h', record[32:34])[0]
    sample_rate_multiplier = unpack('>h', record[34:36])[0]
    sampling_rate = sample_rate_factor / sample_rate_multiplier

    dt = np.dtype(np.float32)
    dt = dt.newbyteorder('>')
    data = np.trim_zeros(np.frombuffer(
        record[-number_of_sample * 4:], dtype=dt), 'b')

    # remove zero at the end
    number_of_sample = len(data)
    starttime = mseed_date_from_header(record)
    endtime = (starttime + (number_of_sample - 1) / float(sampling_rate))

    trace_dict = {}
    stats = {'station_code': station,
             'channel': channel,
             'network': network,
             'start_time': starttime.datetime,
             'end_time': endtime.datetime,
             'sampling_rate': sampling_rate}

    return stats


def write_chunks_to_db(mseed_byte_array, mseed_chunk_size=4096):
    """
    insert the chunks to Redis and Mongo
    :param mseed_byte_array: mseed file byte array
    :param mseed_chunk_size: the mseed chunk size in bytes (default=4096)
    :return: a list of tuple containing (starttime, endtime, stations_code,
    redis_key)
    """

    app = Application()

    redis_settings = app.settings.get('redis_db')
    mongo_settings = app.settings.get('continuous_db')

    logger.info('connecting to Redis')
    redis_conn = Redis(**redis_settings)
    logger.info('successfully connected to Redis')

    logger.info('connecting to MongoDB')
    db_name = mongo_settings.db_name
    collection_name = mongo_settings.traces_collection
    mongo_client = MongoClient(mongo_settings.uri)
    db = mongo_client[db_name]
    collection = db[collection_name]
    logger.info('successfully connected to MongoDB')
    db_ttl = app.settings.get('redis_extra').ttl

    starts = np.arange(0, len(mseed_byte_array), mseed_chunk_size)
    documents = []

    for k, start in enumerate(starts):
        end = start + mseed_chunk_size
        chunk = mseed_byte_array[start:end]

        if len(chunk) < mseed_chunk_size:
            continue
        redis_key = str(uuid4())
        document = extract_mseed_header(chunk)
        document['redis_key'] = redis_key
        document['creation_time'] = datetime.utcnow()
        document['start_time'] = document['start_time']
        document['end_time'] = document['end_time']
        documents.append(document)
        redis_conn.set(redis_key, chunk, ex=db_ttl)

    result = collection.insert_many(document)
