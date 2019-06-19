from spp.core.serializers import serializer
from spp.utils.application import Application
from importlib import reload
from microquake.core import read, read_events
from io import BytesIO
import requests
from loguru import logger

import warnings
warnings.filterwarnings("ignore")

reload(serializer)

app = Application()

seis = serializer.Seismic('processing', app.settings.get('redis_db'),
                          ['fixed_length', 'catalog'])

logger.info('loading mseed data')
mseed_bytes = requests.get("https://permanentdbfilesstorage.blob.core"
                           ".windows.net/permanentdbfilesblob/events/2019-06"
                           "-09T033053.080047Z.context_mseed").content

logger.info('done loading mseed data')

fixed_length_wf = read(BytesIO(mseed_bytes), format='mseed')

logger.info('loading catalogue data')
catalog_bytes = requests.get(
    "https://permanentdbfilesstorage.blob.core.windows.net"
    "/permanentdbfilesblob/events/2019-06-09T033053.047217Z.xml").content

logger.info('done loading catalogue data')

cat = read_events(BytesIO(catalog_bytes), format='quakeml')

seismic_data = {'fixed_length': fixed_length_wf,
                'catalog': cat}

seis.serialize(seismic_data)

assert seis.deserialize() == seismic_data

logger.info('Success!')

