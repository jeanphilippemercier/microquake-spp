from spp.clients.api_client import RequestEvent
from microquake.core.settings import settings
import requests
from urllib.parse import quote
from loguru import logger
from tqdm import tqdm
from pandas import DataFrame
import numpy as np
from microquake.processors import ray_tracer, simple_magnitude

rt_processor = ray_tracer.Processor()
sm_processor = simple_magnitude.Processor()

inventory = settings.inventory

api_base_url = settings.get('api_base_url')

query = 'https://api.microquake.org/api/v1/events?' \
        'event_type=earthquake&status=accepted'

events = []
while query:
    re = requests.get(query)
    # from ipdb import set_trace; set_trace()()
    if not re:
        break
    response = re.json()
    logger.info(f"page {response['current_page']} of "
                f"{response['total_pages']}")

    query = response['next']

    for event in response['results']:
        events.append(RequestEvent(event))

with open('ppv.csv', 'w') as f_out:

    # write the header
    header = 'event id, sensor id, event time epoch, event time utc, ' \
             'event x, event y, event z, sensor x, sensor y, sensor z, ' \
             'moment_magnitude, euclidean distance, distance (m), ' \
             'distance p-ray (m), distance s-ray (m), ' \
             'back-azimuth (degrees), take-off angle (degrees), ppv, ' \
             'ppa, ppd, ssd, energy\n'

    f_out.write(header)

    for event in tqdm(events):

        st = event.get_waveforms().detrend('demean')
        cat = event.get_event()
        try:
            cat[0].preferred_origin().rays[0]
        except TypeError as e:
            logger.info(f'reprocessing event with resource ID {cat[0].resource_id}')
            # e_resource_id = quote(cat[0].resource_id.id, safe='')
            # url = f'{api_base_url}events/{e_resource_id}'
            # resp = requests.patch(url, data={'send_to_bus', True})
            # logger.info(resp.status_code)
            # rt_processor.process(cat=cat.copy())
            # cat = rt_processor.output_catalog(cat.copy())
            continue

        st = st.detrend('demean')
        # st = st.detrend('linear')

        trs = []

        for tr in st:
            if np.any(np.isnan(tr.data)):
                continue
            if np.all(tr.data == 0):
                continue
            tr.stats.network = 'OT'
            trs.append(tr)

        for station in ['6', '8']:
            if len(st.select(station=station)) == 0:
                continue
            st.select(station=station)[0].stats.channel = 'Z'


        st.traces = trs

        st_acc = st.copy().remove_response(output='ACC', inventory=inventory)
        st_vel = st.copy().remove_response(output='VEL', inventory=inventory)
        st_dsp = st.copy().remove_response(output='DISP', inventory=inventory)

        # cat = sm_processor.process(cat=cat, stream=st)

        ev_loc = cat[0].preferred_origin().loc
        origin = cat[0].preferred_origin()
        magnitude = cat[0].preferred_magnitude().mag
        energy = cat[0].preferred_magnitude().energy_joule
        static_stress_drop = cat[0].preferred_magnitude(
                                ).static_stress_drop_mpa

        for tr, tr_acc, tr_vel, tr_dsp in zip(st.composite(),
                                              st_acc.composite(),
                                              st_vel.composite(),
                                              st_dsp.composite()):

            sensor_code = tr.stats.station
            sensor = inventory.select(str(sensor_code))
            reprocessed = False
            try:
                ray_p = origin.get_ray_station_phase(str(sensor_code), 'P')
                ray_s = origin.get_ray_station_phase(str(sensor_code), 'S')
            except Exception as e:
                logger.error(e)
                logger.info(f'missing ray for event {cat[0].resource_id}')

            ppd = np.abs(tr_dsp.data).max()
            ppv = np.abs(tr_vel.data).max()
            ppa = np.abs(tr_acc.data).max()

            euclidean_distance = np.linalg.norm(sensor.loc - origin.loc)
            if ray_p is not None:
                distance_p_ray = ray_p.length
                distance = ray_p.length
                back_azimuth = ray_p.baz
                take_off_angle = ray_p.takeoff_angle
                if ray_p.arrival_id is None:
                    continue
            else:
                continue
                distance_p_ray = 0
                distance = euclidean_distance
                back_azimuth = 0
                take_off_angle = 0

            if back_azimuth is None:
                back_azimuth = 0

            if take_off_angle is None:
                take_off_angle = 0

            back_azimuth = back_azimuth * 180 / np.pi
            take_off_angle = take_off_angle * 180 / np.pi

            if ray_s is not None:
                distance_s_ray = ray_s.length
            else:
                distance_s_ray = 0

            data = f'{event.event_resource_id}, {sensor_code}, ' \
                   f'{str(event.time_epoch)}, ' \
                   f'{event.time_utc}, {event.x:0.2f}, {event.y:0.2f}, ' \
                   f'{event.z:0.2f},' \
                   f' {sensor.x:0.2f}, {sensor.y:0.2f}, {sensor.z:0.2f}, ' \
                   f'{magnitude}, ' \
                   f'{euclidean_distance:0.2f}, {distance:0.2f}, ' \
                   f'{distance_p_ray:0.2f}, {distance_s_ray:0.2f}, ' \
                   f'{back_azimuth:0.2f}, ' \
                   f'{take_off_angle:0.2f}, ' \
                   f'{ppv}, {ppa}, {ppd}, {static_stress_drop}, {energy}\n'

            f_out.write(data)


