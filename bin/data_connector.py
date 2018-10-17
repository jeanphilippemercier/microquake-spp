from spp.data_connector import core
import prometheus_client
from urllib.error import URLError

registry = prometheus_client.CollectorRegistry()
REQUEST_TIME = prometheus_client.Summary('dc_request_processing_seconds', 'Time spent processing request', registry=registry)


if __name__ == "__main__":

    with REQUEST_TIME.time():
        core.load_data()

    try:
        # Prometheus PushGateway server need to be added in config
        prometheus_client.pushadd_to_gateway('localhost:9091', job='data_connector', registry=registry)
    except URLError:
        print("Couldn't connect to prometheus")