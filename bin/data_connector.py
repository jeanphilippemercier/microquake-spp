from spp.data_connector import core
import prometheus_client

registry = prometheus_client.CollectorRegistry()
REQUEST_TIME = prometheus_client.Summary('ims_request_processing_seconds', 'Time spent processing request',registry=registry)

g = prometheus_client.Gauge('ims_finish_seconds', 'Time spent processing request',registry=registry)


if __name__ == "__main__":

    with REQUEST_TIME.time():
        core.load_data()
    g.set_to_current_time()

    # Prometheus PushGateway server need to be added in config
    #prometheus_client.pushadd_to_gateway('localhost:9091', job='data_connector', registry=registry)