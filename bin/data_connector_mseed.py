from spp.data_connector import core
import prometheus_client
import os

registry = prometheus_client.CollectorRegistry()
REQUEST_TIME = prometheus_client.Summary('dc_request_processing_seconds', 'Time spent processing request',registry=registry)


@REQUEST_TIME.time()
def main():
    #core.load_data()

    params = core.get_data_connector_parameters(config_file='data_connector_config_mseed.yaml')
    #elif params['data_source']['type'] == 'local':
    location = params['data_source']['location']
    if os.path.isfile(location):
        print("==> Processing single file %s" % location)
        stream_object = core.get_continuous_local(location, file_name=location)
# write_data destination set in config yaml file:
# params['destination']['type'] = {local, kafka, mongo}
# local : params['destination']['location']
# kafka : kafka_topic = params['kafka']['topic']
#       : kafka_broker = params['kafka']['broker']
# mongo : uri = params['mongo']['uri']
#       : db_name = params['mongo']['db_name']

        destination = params['data_destination']['type']

        print(destination)

        if destination == "local":
            location = params['data_destination']['location']
            print("write to location:[%s]" % location)
            core.write_to_local(stream_object, location)

        elif destination == "kafka":
            brokers=params['kafka']['brokers']
            kafka_topic=params['kafka']['topic']
            core.write_to_kafka(stream_object, brokers, kafka_topic)

        elif destination == "mongo":
            uri = params['mongo']['uri']
            db_name = params['mongo']['db_name']
            core.write_to_mongo(stream_object, uri=uri, db_name=db_name)
    else:
        print("shouldn't be here!")


if __name__ == "__main__":

    main()

    # Prometheus PushGateway server need to be added in config
    prometheus_client.pushadd_to_gateway('localhost:9091', job='data_connector', registry=registry)