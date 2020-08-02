from datetime import datetime, timedelta
from spp.utils.application import Application
from spp.clients.ims import web_client
import faust
from dateutil.parser import parse
from pytz import utc
from model import seismic_data
from io import BytesIO
import numpy as np
from struct import unpack
from microquake.io.waveform import mseed_date_from_header


app = faust.App('event_detection', broker='broker:9092',
                value_serializer='raw', topic_partition=8)

spp_app = Application()

base_url = spp_app.settings.get('data_connector').path
tz = spp_app.get_time_zone()

inventory = spp_app.get_inventory()
station_codes = [station.code for station in inventory.stations()]

offset = 2 * 60  # offset in second from now to set the starttime this will
# need to be in a config file

mseed_chunk_size = 4096

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


# def write_mseed_chunk(stream, brokers, mseed_chunk_size=4096):
#
#     from io import BytesIO
#     from microquake.core import read
#     import numpy as np
#     from uuid import uuid4
#     from confluent_kafka import Producer
#     import time
#
#     p = Producer({'bootstrap.servers': 'broker:9092'})
#     # kproducer = Producer(
#     #     {"bootstrap.servers": brokers}
#     # )
#
#     bio = BytesIO()
#     stream.write(bio, format='mseed')
#
#     mseed_byte_array = bio.getvalue()
#
#     starts = np.arange(0, len(mseed_byte_array), mseed_chunk_size)
#
#     traces = []
#     t0 = time.time()
#     for k, start in enumerate(starts):
#         p.poll(0)
#         end = start + mseed_chunk_size
#         chunk = mseed_byte_array[start:end]
#         header = extract_mseed_header(chunk)
#         timestamp = int(header['start_time'].timestamp() * 1e3)
#         p.produce('continuous_data', chunk, header['station_code'],
#                   timestamp=timestamp)
#     p.flush()
#     t1 = time.time()
#     print(t1 - t0)
#

wlen = 2000  # window length in mili-second
overlap = 200  # overlap in mili-second

mseed_stream_station = app.topic('mseed_stream_station',
                                 key_type=str,
                                 value_type=seismic_data)

mseed_stream_window = app.topic('mseed_stream_window',
                                key_type=str,
                                value_type=seismic_data)
# mseed_stream_time
@app.agent(mseed_stream_station)
async def mseed_station(messages):
    async for message in messages:
        pass
        # print (message)


number_of_stream = app.Table('total_number_of_stream', default=int,
                             partitions=1)
@app.agent(mseed_stream_window)
async def mseed_window(messages):
    async for key, message in messages.items():
        # pass
        number_of_stream[key] += 1
        print (key, number_of_stream[key])



continuous_data = app.topic('continuous_data', value_type=str)
@app.agent(continuous_data)
async def data_connector(messages):
    async for key, message in messages.items():
        message_list = message.split(',')
        starttime = parse(message_list[0]).replace(tzinfo=tz)
        try:
            endtime = parse(message_list[1]).replace(tzinfo=tz)
        except:
            endtime = datetime.now().replace(tzinfo=tz)

        station_code = np.int(message_list[2])
        try:
            st = web_client.get_continuous(base_url, starttime, endtime,
                                           [station_code], tz)

            bio = BytesIO()
            st.write(bio, format='mseed')
            mseed_byte_array = bio.getvalue()
            starts = np.arange(0, len(mseed_byte_array), mseed_chunk_size)

            for k, start in enumerate(starts):
                end = start + mseed_chunk_size
                chunk = mseed_byte_array[start:end]
                header = extract_mseed_header(chunk)
                timestamp = int(header['start_time'].timestamp() * 1e3)

                await mseed_station.send(key=str(station_code), value=chunk,
                                         timestamp=timestamp)

                w_timestamp = int(np.floor(timestamp / wlen) * wlen / 1000)
                await mseed_window.send(key=str(w_timestamp), value=chunk,
                                        timestamp=w_timestamp)

                if timestamp - w_timestamp < overlap:
                    ts = int((w_timestamp - wlen) / 1000)
                    await mseed_window.send(key=str(ts), value=chunk,
                                            timestamp=ts)

            starttime = st[0].stats.endtime.datetime.replace(
                tzinfo=utc).astimezone(tz)
            message = '%s, %s, %s' % (starttime, '', station_code)
            await data_connector.send(key=str(station_code), value=message)
            print(st)

        except:
            message = '%s, %s, %s' % (starttime, endtime, station_code)
            await data_connector.send(key=str(station_code), value=message)


@app.task(on_leader=True)
async def initialize():

    endtime = datetime.now()
    starttime = endtime - timedelta(seconds=offset)
    for station_code in station_codes[40:44]:
        print('PUBLISHING ON LEADER!')
        message = '%s, %s, %s' % (starttime, endtime, station_code)
        await data_connector.send(key=station_code, value=message)

# if __name__ == '__main__':
# initialize(starttime, endtime, station_codes)





# kproducer = Producer(
#             {"bootstrap.servers": app.settings.get('kafka').brokers}
#             )
# p = Producer({'bootstrap.servers': 'broker:9092'})
