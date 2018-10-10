from spp.data_connector import core
import prometheus_client

REQUEST_TIME = prometheus_client.Summary('request_processing_seconds', 'Time spent processing request')

@REQUEST_TIME.time()
def main():
    core.load_data()


if __name__ == "__main__":

    prometheus_client.start_http_server(8001)

    main()
