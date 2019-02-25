"""

This convenience module will insert api data into the SPP processing pipeline
to kick it off.

A kafka message bus connects modules to each other. See the settings.toml to
determine the topic where a message is sent

Example usage:

python ./bin/send_api_data_to_channel.py smi:local/97f39d25-db59-40fb-bcf8-57de70589fd1

"""


import logging
import sys
import time
from typing import List, Tuple

from obspy.core.event.catalog import Catalog

from microquake.core.stream import Stream
from spp.utils.kafka_redis_application import KafkaRedisApplication
from spp.utils.seismic_client import get_event_by_id

__module_name__ = "initializer"


def retrieve_api_event(
    api_base_url: str, event_id: str, stations_black_list: List[str]
) -> Tuple[Catalog, Stream]:
    """
    Retrieve a catalog and filtered waveform list from the api
    """

    request = get_event_by_id(api_base_url, event_id)
    if request is None:
        raise RuntimeError()
    catalog = request.get_event()
    waveform_stream = request.get_waveforms()

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
    logging.getLogger("send_api_data_to_channel").info("Initialising application")

    app = KafkaRedisApplication(
        module_name=__module_name__, processing_flow_name="automatic"
    )
    logger = app.get_logger("send_api_data_to_channel", "send_api_data_to_channel.log")
    logger.info("Initialised application")

    event_id = sys.argv[1:][0]

    logger.info("Retrieving event and waveforms from API: %s", event_id)
    catalog, waveform_stream = retrieve_api_event(
        app.settings.seismic_api.base_url, event_id, app.settings.sensors.black_list
    )

    logger.info("Sending event to message bus")
    send_to_msg_bus(app, catalog, waveform_stream)

    logger.info("Sent event to message bus, exiting")
