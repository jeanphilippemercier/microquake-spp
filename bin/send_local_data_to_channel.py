"""

This convenience module will insert local data into the SPP processing pipeline
to kick it off.

A kafka message bus connects modules to each other. See the settings.toml to
determine the topic where a message is sent

Example usage:

python send_local_data_to_channel.py ./tmp/2018-11-08T10-21-49.846299Z.mseed ./tmp/2018-11-08T10-21-49.846299Z.quakeml

"""


import logging
import sys
import time
from typing import List, Tuple

from obspy.core.event.catalog import Catalog

from microquake.core import read_events
from microquake.core.stream import Stream, read
from spp.utils.kafka_redis_application import KafkaRedisApplication

__module_name__ = "initializer"


def retrieve_local_event(
    local_mseed_location: str,
    local_quakeml_location: str,
    stations_black_list: List[str],
) -> Tuple[Catalog, Stream]:
    """
    Retrieve a catalog and filtered waveform list from the local file system
    """

    with open(local_quakeml_location, "rb") as event_file:
        catalog = read_events(event_file, format="QUAKEML")

    with open(local_mseed_location, "rb") as waveform_file:
        waveform_stream = read(waveform_file, format="MSEED")

    for trace in waveform_stream:
        if trace.stats.station not in stations_black_list:
            waveform_stream.remove(trace)

    return catalog, waveform_stream


def send_to_msg_bus(
    app: KafkaRedisApplication, catalog: Catalog, waveform_stream: Stream
):
    """
    Send a catalog and waveform list to the msg bus
    """
    app.send_message(catalog, waveform_stream)

    # send_message will send to kafka via producer.produce, an async function.
    # We should not exit out immediately so that thie function has some time to
    # complete.
    # An ideal solution would block until the kafka client's on_delivery callback
    # is called.
    time.sleep(2)


if __name__ == "__main__":
    logging.getLogger("send_local_data_to_channel").info("Initialising application")

    app = KafkaRedisApplication(
        module_name=__module_name__, processing_flow_name="automatic"
    )
    logger = app.get_logger(
        "send_local_data_to_channel", "send_local_data_to_channel.log"
    )
    logger.info("Initialised application")

    local_mseed_location = sys.argv[1:][0]
    local_quakeml_location = sys.argv[1:][1]

    logger.info(
        "Retrieving event and waveforms from local file system: %s %s",
        local_mseed_location,
        local_quakeml_location,
    )
    catalog, waveform_stream = retrieve_local_event(
        local_mseed_location, local_quakeml_location, app.settings.sensors.black_list
    )

    logger.info("Sending event to message bus")
    send_to_msg_bus(app, catalog, waveform_stream)

    logger.info("Sent event to message bus, exiting")
