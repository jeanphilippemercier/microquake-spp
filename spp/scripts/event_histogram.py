from microquake.clients.api_client import get_events_catalog
from microquake.core.settings import settings
from microquake.core.helpers.time import get_time_zone
from datetime import datetime, timedelta
from obspy.core import UTCDateTime
from dateutil.parser import parse
import matplotlib.pyplot as plt
from matplotlib.dates import date2num
import numpy as np
import pandas as pd
import argparse
import requests
from pytz import utc
import urllib
from loguru import logger
import matplotlib.dates as mdates
from matplotlib import dates
from matplotlib import ticker

parser = argparse.ArgumentParser(description='create a event histogram '
                                             'between a start date and an '
                                             'end date')

parser.add_argument('--starttime', type=str, help='start time in system '
                                                  'local time')
parser.add_argument('--endtime', type=str, help='end time in system local '
                                                'time')

parser.add_argument('--bin_size', type=str,
                    help='binning period\n possible values: d, w, m '
                         'representing day, week, and month, respectively ('
                         'default=d')

parser.add_argument('--output_file', type=str, help='file path with extension')

dpi = 400

args = parser.parse_args()

if args.bin_size is not None:
    bin_size = args.bin_size
else:
    bin_size = 'd'

if bin_size == 'd':
    fact = 2
elif bin_size == 'w':
    fact = 2
elif bin_size == 'm':
    fact = 2

output_file = args.output_file

tz = get_time_zone()

base_url = settings.get('api_base_url')
if base_url[-1] == '/':
    base_url = base_url[:-1]
url = f'{base_url}/events'

start_time = UTCDateTime(parse(args.starttime).replace(tzinfo=tz))
end_time = UTCDateTime(parse(args.endtime).replace(tzinfo=tz))

# getting data from the API
request_dict = {'time_utc_after': str(start_time),
                'time_utc_before': str(end_time),
                'event_type': 'earthquake',
                'status': 'accepted'}
tmp = urllib.parse.urlencode(request_dict)
query = f'{url}?{tmp}'

magnitudes = []
times = []

while query:
    re = requests.get(query)
    if not re:
        logger.error('Problem communicating with the API')
        exit()
    response = re.json()
    logger.info(f"page {response['current_page']} of "
                f"{response['total_pages']}")
    for event in response['results']:
        magnitudes.append(event['magnitude'])
        t = datetime.fromtimestamp(event['time_epoch']/1e9)
        t = t.replace(tzinfo=utc).astimezone(tz=tz)
        times.append(t)

    query = response['next']


# from ipdb import set_trace; set_trace()

df = pd.DataFrame()
df['magnitude'] = np.array(magnitudes)
df['time'] = np.array(times)

df = df.set_index('time')

df['c1'] = np.array(df['magnitude'] < -1).astype(np.int)
df['c2'] = np.array((-1 <= df['magnitude']) &
                    (df['magnitude'] < 0)).astype(
    np.int)
df['c3'] = np.array((0 <= df['magnitude']) &
                    (df['magnitude'] < 1)).astype(np.int)
df['c4'] = np.array(1 <= df['magnitude']).astype(np.int)

df_cat = df.resample(f'1{bin_size}', label='left', closed='left').sum()

fig = plt.figure(1)
plt.clf()
ax = plt.subplot(111)
df_cat[['c1', 'c2', 'c3', 'c4']].plot.bar(stacked=True, ax=ax)

# ax.xaxis.set_major_locator(mdates.DayLocator(interval=5))
ticklabels = [''] * len(df_cat.index)
ticklabels[::fact] =[item.strftime('%b %d') for item in df_cat.index[::fact]]
ax.xaxis.set_major_formatter(ticker.FixedFormatter(ticklabels))
# plt.gcf().autofmt_xdate()

# ax.format_xdata = mdates.DateFormatter('%Y-%m-%d')
fig.autofmt_xdate()

ax.legend(['$M_w < -1$', '$M_w = [-1, 0)$', '$M_w = [0, 1)$', '$M_w > 1$'])

plt.xlabel('time')
plt.ylabel('number of events')

plt.tight_layout()
plt.savefig(output_file, dpi=dpi)

plt.show()

# fig, ax = plt.subplots()
# p4 = ax.bar(df_cat.index, df_cat['c4'], width=0.8*fact, label='$M_w > 2$')
# p3 = ax.bar(df_cat.index, df_cat['c3'], width=0.8*fact, label='$M_w = [0, '
#                                                                '1)$')
# p2 = ax.bar(df_cat.index, df_cat['c2'], width=0.8*fact, label='$M_w = [-1, '
#                                                                '0)$')
# p1 = ax.bar(df_cat.index, df_cat['c1'], width=0.8*fact, label='$M_w < -1$')
#
# ax.set_ylabel('frequency')
# plt.xticks(rotation=20, ha='right')
# plt.legend()
# # ax.set_xlim([date2num(starttime.datetime), date2num(endtime.datetime)])
# plt.tight_layout()
# plt.savefig(output_file)
# plt.show()
# # category_boundaries = [-2, -1, 0, 1, 2]
# #
# # c1 = times[magnitudes < -1]
# # c2 = times[(magnitudes < 0) and (magnitudes >= -1)]
# # c3 = times[(magnitudes < 1) and (magnitudes >= 0)]
# # c4 = times[(magnitudes ]
#
#







