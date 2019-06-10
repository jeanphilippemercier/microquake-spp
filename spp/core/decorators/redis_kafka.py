import faust
from spp.utils.application import Application
from redis import StrictRedis
from spp.core.serializers.pipeline import pipeline_message
# from spp.core.serializers.seismic import Seismic


# spp_app = Application('spp')
processing_flow = 'automatic'
module_name = 'interloc'
topic_name = 'spp.%s.%s' % (processing_flow, module_name)
app = faust.App('spp')
input_topic = app.topic(topic_name)# , value_type=pipeline_message)


# redis_conn = StrictRedis(**app.settings.get('redis_db'))

# processor = Processor(module_name, app=spp_app,
#                       module_type=None)

@app.agent(input_topic)
# @app.agent()
async def seismic_data(events):
    """
    help
    """
    async for event in events:
        print(event)
        yield event

if __name__ == '__main__':
    app.main()

# def redis_kafka_agent(Processor, processing_flow, module_name, message_type,
#                       serializer, deserializer):
#     """
#     decorator for the processor that creates an agent
#     :param processor:
#     :param processing_flow:
#     :param module_name:
#     :param message_type:
#     :return:
#     """
#
#     spp_app = Application('spp')
#     topic_name = 'spp.%s.%s' % (processing_flow, module_name)
#     app = faust.App('spp')
#     input_topic = app.topic(topic_name, value_type=message_type)
#
#
#     redis_conn = StrictRedis(**app.settings.get('redis_db'))
#
#     processor = Processor(module_name, app=spp_app,
#                           module_type=None)
#
#     @app.agent(input_topic)
#     async def task(events):
#         async for event in events:
#             print(event)
#
#
#
#
