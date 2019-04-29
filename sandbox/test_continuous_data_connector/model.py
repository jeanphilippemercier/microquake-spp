import faust
from struct import unpack
import numpy as np
from microquake.io.waveform import mseed_date_from_header
from typing import Any

from faust.serializers import codecs

class seismic_data(faust.Record, serializer='mseed'):
    record: bytes
    station_code: str
    channel_code: str
    network: str
    number_of_sample: int
    sample_rate_factor: int
    sample_rate_multiplier: int
    sampling_rate: float
    number_of_sample: int
    starttime: float
    endtime: float
    time: float


class Mseed(codecs.Codec):
    def _dumps(self, obj: Any) -> bytes:
        return obj['record']

    def _loads(self, s: bytes) -> Any:
        d = {}
        record = s
        d['record'] = s
        d['station_code'] = unpack('5s', record[8:13])[0].strip().decode()
        d['channel_code'] = unpack('3s', record[15:18])[0].strip().decode()
        d['network'] = unpack('2s', record[18:20])[0].strip().decode()
        d['number_of_sample'] = unpack('>H', record[30:32])[0]
        d['sample_rate_factor'] = unpack('>h', record[32:34])[0]
        d['sample_rate_multiplier'] = unpack('>h', record[34:36])[0]
        d['sampling_rate'] = d['sample_rate_factor'] / \
                             d['sample_rate_multiplier']

        dt = np.dtype(np.float32)
        dt = dt.newbyteorder('>')
        data = np.trim_zeros(np.frombuffer(
            record[-d['number_of_sample'] * 4:], dtype=dt), 'b')

        # remove zero at the end
        d['number_of_sample'] = len(data)
        starttime = mseed_date_from_header(record).datetime.timestamp()
        d['starttime'] = starttime
        d['endtime'] = starttime + (d['number_of_sample'] - 1) / \
                       float(d['sampling_rate'])
        d['time'] = (d['starttime'] + d['endtime']) / 2
        return d


codecs.register("mseed", Mseed())