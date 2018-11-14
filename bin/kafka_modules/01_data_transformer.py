# from importlib import reload
# import numpy as np
# import glob
# from microquake.core import read
# from spp.utils.kafka import KafkaHandler
# from io import BytesIO
# from microquake.io import msgpack
# from struct import unpack
# from datetime import datetime
# from microquake.core.util import tools
# from microquake.io import waveform
import os
import subprocess

from spp.utils.application import Application

app = Application()

# env destination="mseed_1sec" source="mseed-blocks" brokers="localhost" java -jar $SPP_COMMON/jars/mseed-streams-1.0-SNAPSHOT.jar
jar_path = os.path.join(app.common_dir, "jars", "mseed-streams-1.0-SNAPSHOT.jar")
params = app.settings.transformer

cmd = f"env source={params.kafka_consumer_topic} \
        destination={params.kafka_producer_topic} \
         brokers=localhost java -jar {jar_path}"
subprocess.call(cmd, shell=True)
