import argparse
from spp.clients import api_client
from microquake.core.settings import settings
from datetime import datetime
from dateutil.relativedelta import relativedelta
from dateutil import parser as dtparser
import filecmp
import os
import shutil
from microquake.helpers.logging import logger

api_base_url = settings.get('api_base_url')
api_user = settings.get('api_user')
api_password = settings.get('api_password')

event_type_lookup = api_client.get_event_types(api_base_url,
                                               username=api_user,
                                               password=api_password)

reverse_event_lookup = {}
for key in event_type_lookup:
    reverse_event_lookup[event_type_lookup[key]] = key

parser = argparse.ArgumentParser(description='collects data from the API'
                                             'and write the data in a csv file'
                                             'in a format compatible with'
                                             'mXrap')

parser.add_argument('-t', '--time_delay', type=float)
parser.add_argument('-u', '--unit', type=str,
                    help='units - h -- hours, d -- days, m -- month(s), '
                         'y -- year')
parser.add_argument('-o', '--output', type=str, help='output file')

args = parser.parse_args()

time_delay = args.time_delay
time_unit = args.unit
output_file = args.output

if time_delay is not None:
    end_time = datetime.utcnow()

    time_unit_lookup = {'h': 'hours', 'd': 'days', 'w': 'weeks',
                        'm': 'months', 'y': 'years'}

    kwargs = {time_unit_lookup[time_unit]: time_delay}

    start_time = end_time - relativedelta(**kwargs)
else:
    end_time = None
    start_time = None

sc = api_client.SeismicClient(api_base_url, api_user, api_password)
response, events = sc.events_list(start_time, end_time,
                                  event_type='seismic event',
                                  status='accepted')

tmp_out_file = 'tmp.csv'
with open(tmp_out_file, 'w') as fout:
    header = 'Event UUID, event time (UTC), insertion time (UTC), ' \
             'modification time (UTC), x, y, z, location uncertainty (m), ' \
             'event type, evaluation mode, evaluation status (Quakeml), ' \
             'number of picks, magnitude, magnitude type, ' \
             'scalar seismic moment (N m), corner frequency (Hz),' \
             'potency (m**3), energy (j), energy p (j), energy s (j),' \
             'static stress drop (MPa), apparent stress (Pa)\n'
    fout.write(header)
    for event in events:

        if event.preferred_magnitude is None:
            magnitude = event.magnitude
            seismic_moment = ''
            potency = ''
            energy = ''
            energy_p = ''
            energy_s = ''
            ssd = ''
            astress = ''
        else:
            magnitude = event.magnitude
            seismic_moment = event.preferred_magnitude["seismic_moment"]
            potency = event.preferred_magnitude["potency_m3"]
            energy = event.preferred_magnitude["energy_joule"]
            energy_p = event.preferred_magnitude["energy_p_joule"]
            energy_s = event.preferred_magnitude["energy_s_joule"]
            ssd = event.preferred_magnitude["static_stress_drop"]
            astress = event.preferred_magnitude["apparent_stress_pa"]

        event_time_utc = event.time_utc.strftime('%Y-%m-%dT%H:%M:%S.%f')[0:-3]
        if event.insertion_timestamp is not None:
            insertion_timestamp = event.insertion_timestamp.strftime(
                '%Y-%m-%dT%H:%M:%S.%f')[0:-3]
        else:
            insertion_timestamp = event_time_utc

        modification_timestamp = event.modification_timestamp.strftime(
            '%Y-%m-%dT%H:%M:%S.%f')[0:-3]

        out_str = f'{event.event_resource_id}, {str(event_time_utc)}, ' \
                  f'{str(insertion_timestamp)}, ' \
                  f'{str(modification_timestamp)}, ' \
                  f'{event.x}, {event.y}, {event.z}, {event.uncertainty}, ' \
                  f'{reverse_event_lookup[event.event_type]}, ' \
                  f'{event.evaluation_mode}, {event.status}, {event.npick}, ' \
                  f'{magnitude}, ' \
                  f'moment magnitude, ' \
                  f'{seismic_moment}, ' \
                  f'{event.corner_frequency}, ' \
                  f'{potency}, ' \
                  f'{energy}, ' \
                  f'{energy_p}, ' \
                  f'{energy_s}, ' \
                  f'{ssd}, ' \
                  f'{astress}\n'

        fout.write(out_str)

# comparing the new file with the current file. If the files are not equal
# or if the file does not exist, the tmp file will be moved.

if os.path.exists(output_file):
    logger.info('file already exists...')
    if not filecmp.cmp(tmp_out_file, output_file):
        logger.info('but files are different')
        shutil.move(tmp_out_file, output_file)
else:
    logger.info('file is being created')
    shutil.move(tmp_out_file, output_file)




