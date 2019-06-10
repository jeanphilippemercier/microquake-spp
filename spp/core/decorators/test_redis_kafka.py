import faust
from spp.core.decorators import redis_kafka
from spp.core.serializers.pipeline import pipeline_message

app = faust.App('spp')

module_name = 'interloc'
processing_flow = 'automatic'

topic_name = 'spp.%s.%s' % (processing_flow, module_name)

output_topic = app.topic(topic_name, value_type=pipeline_message)

@app.timer(interval=5.0)
async def send_data(output_topic):
    await output_topic.send(
        value=pipeline_message(processing_flow='automatic',
                               run_id='random_string', processing_step=0),
    )

if __name__ == '__main__':
    app.main()