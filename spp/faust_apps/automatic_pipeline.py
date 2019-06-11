import faust
from datetime import datetime
from spp.utils.application import Application
from spp.pipeline.interloc import Processor
from spp.core.serializers.serializer import Seismic
from uuid import uuid4
from loguru import logger
import requests
from io import BytesIO
from microquake.core import read, read_events, UTCDateTime
from microquake.core.event import Origin, AttribDict

spp_app = Application()

app = faust.App('spp_automatic_pipeline', broker='broker')

modules = ['interloc', 'picker', 'nlloc', 'measure_amplitudes', 'measure_smom',
           'measure_smom', 'focal_mechanism', 'measure_energy', 'magnitude',
           'event_database']

partition = spp_app.settings.get('kafka').topic_partition
redis_settings = spp_app.settings.get('redis_db')


class PipelineMessage(faust.Record):
    process_id: str
    processing_step: int


def origin_from_response(response):
    res = response
    x = res['x']
    y = res['y']
    z = res['z']
    vmax = res['vmax']
    normed_vmax = res['normed_vmax']
    method = res['method']
    event_time = UTCDateTime(datetime.fromtimestamp(res['event_time']))

    origin = Origin(x=x, y=y, z=z, time=event_time,
                    method_id=method, evalution_status="preliminary",
                    evaluation_mode="automatic")

    origin.extra.interloc_vmax = \
       AttribDict({'value': vmax, 'namespace': 'MICROQUAKE'})

    origin.extra.interloc_normed_vmax \
        = AttribDict({'value': normed_vmax, 'namespace': 'MICROQUAKE'})

    return origin


topics = {}
for module in modules:
    topics[module] = app.topic(module, value_type=PipelineMessage)

@app.agent(topics['interloc'])
async def interloc(events):
    interloc = Processor('interloc', app=spp_app)
    async for event in events:
        print(event)
        source = Seismic(event.process_id, redis_settings,
                         types=['fixed_length', 'catalog'])
        data = source.deserialize()
        print(data.keys())
        print(data)
        response = interloc.process(stream=data['fixed_length'])

        cat = data['catalog']

        origin = origin_from_response(response)
        cat[0].origins.append(origin)
        cat[0].preferred_origin_id = origin.resource_id.id

        seismic_data = {'catalog': cat}
        source.serialize(seismic_data)

        event.processing_step += 1

        print(response)

        value = await picker.ask()

        async for reply in picker.map([pm, pm, pm]):
            print(f'RECEIVED REPLY: {reply!r}')

        await topics['picker'].send(value=event)


@app.agent(topics['picker'])
async def picker(events):
    async for event in events:
        print(event)



@app.timer(10, on_leader=True)
async def create_message():
    process_id = str(uuid4())
    pm = PipelineMessage(process_id=process_id, processing_step=0)

    seismic = Seismic(process_id, redis_settings,
                    ['fixed_length', 'catalog'])

    logger.info('loading mseed data')
    # mseed_bytes = requests.get("https://permanentdbfilesstorage.blob.core"
    #                            ".windows.net/permanentdbfilesblob/events/2019-06"
    #                            "-09T033053.080047Z.mseed").content
    #
    # with open('test_data.mseed', 'wb') as fout:
    #     fout.write(mseed_bytes)

    with open('test_data.mseed', 'rb') as fin:
        mseed_bytes = fin.read()

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

    # from pdb import set_trace; set_trace()
    seismic.serialize(seismic_data)

    await topics['interloc'].send(value=pm)

if __name__ == '__main__':
    app.main()


# [
# { module='interloc', input='data_automatic', output='picker'},
# { module='picker', input='picker', output='nlloc'},
# { module='nlloc', input='nlloc', output='measure_amplitudes'},
# { module='measure_amplitudes', input='measure_amplitudes', output='measure_smom'},
# { module='measure_smom', input='measure_smom', output='focal_mechanism'},
# { module='focal_mechanism', input='focal_mechanism', output='measure_energy'},
# { module='measure_energy', input='measure_energy', output='magnitude'},
# { module='magnitude', input='magnitude', output='magnitude_frequency'},
# { module='magnitude', type='frequency', input='magnitude_frequency', output='event_database'},
# { module='event_database', input='event_database'}
# ]