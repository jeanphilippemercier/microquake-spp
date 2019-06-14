import faust
from datetime import datetime
from spp.utils.application import Application
from spp.pipeline import interloc, picker
# from spp.core.serializers.serializer import Seismic
from serializer import Seismic
from uuid import uuid4
from loguru import logger
import requests
from io import BytesIO
from microquake.core import read, read_events, UTCDateTime
from microquake.core.event import Origin, AttribDict
from typing import AsyncIterable
import pickle
from models import PipelineMessage
import numpy as np

spp_app = Application()

app = faust.App('spp_automatic_pipeline', reply_create_topic=True,
                broker='broker', topic_partitions=10)

modules = ['interloc', 'picker', 'nlloc', 'measure_amplitudes', 'measure_smom',
           'measure_smom', 'focal_mechanism', 'measure_energy', 'magnitude',
           'event_database']

# partition = spp_app.settings.get('kafka').topic_partition
redis_settings = spp_app.settings.get('redis_db')

# !!!!! IT WILL BE CRUCIAL TO MANAGE MEMORY ISSUE RELATED TO REDIS. THE
# OBJECT IN THE DATABASE DO NOT HAVE EXPIRATION.
source = Seismic(redis_settings)

@app.agent()
async def orchestrator(events) -> None:
    async for event in events:

        # STEP - 1: INTERLOC, preliminary event location
        logger.info('Pipeline STEP - 1: Interloc')
        await interloc_agent.ask(value=event)

        # # STEP - 2: PICKERS, P- and S- wave arrival picking
        logger.info('Pipeline STEP - 2: Pickers')

        await pickers_election_agent.ask(value=event)

        # # STEP - 3: NONLINLOC, Non linear location method using picks

@app.agent()
async def interloc_agent(events) -> None:
    processor = interloc.Processor('interloc', app=spp_app)
    async for event in events:
        process_id = event['process_id']
        data = source.deserialize(process_id, ['fixed_length', 'catalog'])
        response = processor.process(stream=data['fixed_length'])

        cat = data['catalog']

        cat_out = processor.output_catalog(cat)

        seismic_data = {'catalog': cat_out}
        source.serialize(process_id, seismic_data)

        yield response


@app.agent()
async def pickers_election_agent(events):
    async for event in events:

        process_id = event['process_id']
        msg_pk_hf = PipelineMessage(process_id=process_id,
                                    module_type='high_frequencies')
        msg_pk_mf = PipelineMessage(process_id=process_id,
                                    module_type='medium_frequencies')
        msg_pk_lf = PipelineMessage(process_id=process_id,
                                    module_type='low_frequencies')
        msg_list = [msg_pk_hf, msg_pk_mf, msg_pk_lf]
        results = await picker_agent.join(msg_list)

        nb_picks = [result['nb_picks'] for result in results]
        res_keys = [result['result_catalog_key'] for result in results]
        imax = np.argsmax(nb_picks)
        res_key = res_key[imax]
        data = source.deserialize(process_id, res_key)

        pickers = ['high_frequencies', 'medium_frequencies', 'low_frequencies']

        logger.info('and the best picker was ... the % picker' % pickers[imax])

        source.serialize(process_id, {'catalog': data['catalog']})

        yield pickers[imax]


@app.agent()
async def picker_agent(events):
    async for event in events:
        print(event)
        processor = picker.Processor('picker', app=spp_app,
                                     module_type=event['module_type'])
        source = Seismic(redis_settings)
        process_id = event['process_id']
        data = source.deserialize(process_id, ['fixed_length', 'catalog'])
        print(data.keys())
        response = processor.process(stream=data['fixed_length'],
                                     cat=data['catalog'])
        # print(response)
        catalog_key = '%s_picker_%s' % (process_id, event['module_type'])
        cat = processor.output_catalog(data['catalog'])
        output_dict = {'catalog' : cat}

        logger.info('%s picker yielded %d picks' % (event['module_type'],
                                                    len(response['picks'])))

        source.serialize(process_id, output_dict)

        result = {'nb_picks': len(response['picks']),
                  'result_catalog_key': catalog_key}

        yield result


@app.agent()
async def nll_agent(events):
    async for event in events:
        pass



@app.timer(30, on_leader=True)
async def create_message():
    process_id = str(uuid4())
    pm = PipelineMessage(process_id=process_id,
                         module_type=None)

    seismic = Seismic(redis_settings)

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
    # catalog_bytes = requests.get(
    #     "https://permanentdbfilesstorage.blob.core.windows.net"
    #     "/permanentdbfilesblob/events/2019-06-09T033053.047217Z.xml").content
    #
    # with open('test_data.xml', 'wb') as fout:
    #     fout.write(catalog_bytes)

    with open('test_data.xml', 'rb') as fin:
        catalog_bytes = fin.read()

    logger.info('done loading catalogue data')

    cat = read_events(BytesIO(catalog_bytes), format='quakeml')

    seismic_data = {'fixed_length': fixed_length_wf,
                    'catalog': cat}

    # from pdb import set_trace; set_trace()
    seismic.serialize(process_id, seismic_data)

    await orchestrator.send(value=pm)

import asyncio
if __name__ == '__main__':
    loop = asyncio.get_event_loop()
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

