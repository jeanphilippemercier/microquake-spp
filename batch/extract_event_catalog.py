from spp.clients import api_client
from microquake.core.settings import settings
from microquake.core.helpers.time import get_time_zone
from obspy.core import UTCDateTime
from dateutil.parser import parse
import argparse
from importlib import reload
from microquake.helpers.logging import logger

reload(api_client)

api_base_url = settings.get('API_BASE_URL')
tz = get_time_zone()

parser = argparse.ArgumentParser(description='extract the evp equivalent '
                                             'file from the seismic system '
                                             'the result is pipe to stdout')

parser.add_argument('--starttime', type=str, help='start time in system '
                                                  'local time')
parser.add_argument('--endtime', type=str, help='end time in system local '
                                                'time')

parser.add_argument('--output', type=str, help='output file')

# parser.add_argument('--mxrap', type=bool, help='MxRap output')

args = parser.parse_args()

start_time = UTCDateTime(parse(args.starttime).replace(tzinfo=tz))
end_time = UTCDateTime(parse(args.endtime).replace(tzinfo=tz))

# start_time = UTCDateTime(2019, 12, 1)
# end_time = UTCDateTime(2019, 12, 31)

res = api_client.get_catalog(api_base_url, start_time, end_time,
                             event_type='seismic event', status='accepted')

header = ('Event UUID, event time (UTC), insertion time (UTC), modification '
          'time '
          '(UTC), '
          'x, y, z, location uncertainty (m), event type, evaluation '
          'mode, evaluation status (Quakeml), number of picks, '
          'magnitude, magnitude type, scalar seismic moment (N m), corner '
          'frequency (Hz), potency (m**3), energy (j), energy p (j), '
          'energy s (j), static stress drop (MPa), apparent stress (Pa)\n')

with open(args.output, 'w') as ofile:
    ofile.write(header)
    for i, re in enumerate(res):
        perc_comp = (i + 1) / len(res)
        print(f'Processing {i + 1} of {len(res)}: {perc_comp:0.0%}  '
              f'completed \r')
        # logger.info(f'processing event {i + 1} of {len(res)}')
        # event_id = re.event_resource_id
        # magnitude_id = re.preferred_magnitude_id
        # resp = api_client.get_magnitude(api_base_url, magnitude_id)[-1]

        # Brute force !!!
        cat = re.get_event()

        mag = cat[0].preferred_magnitude()

        if mag is None:
            mag = cat[0].magnitudes[-1]

        event_types_lookup = api_client.get_event_types(api_base_url)

        inverted_lookup = {}
        for key in event_types_lookup:
            inverted_lookup[event_types_lookup[key]] = key

        event_type = inverted_lookup[re.event_type]

        out_str = (f'{re.event_resource_id}, '
                   f'{str(re.time_utc)[:-4]}Z, '
                   f'{str(re.insertion_timestamp)[:-4]}Z, '
                   f'{str(re.modification_timestamp)[:-4]}Z, '
                   f'{re.x:0.2f}, {re.y:0.2f}, {re.z:0.2f}, '
                   f'{re.uncertainty}, '
                   f'{event_type}, {re.evaluation_mode}, {re.status}, '
                   f'{re.npick}, '
                   f'{re.magnitude:0.2f}, {re.magnitude_type}, '
                   f'{mag.seismic_moment}, {mag.corner_frequency_hz}, '
                   f'{mag.potency_m3}, '
                   f'{mag.energy_joule}, {mag.energy_p_joule}, '
                   f'{mag.energy_s_joule}, '
                   f'{mag.static_stress_drop_mpa}, '
                   f'{mag.apparent_stress}\n')

        ofile.write(out_str)

        # print(out_str)



# for re in res:


