from spp.utils.seismic_client import get_events_catalog
from microquake.core import UTCDateTime
from spp.utils.application import Application
import matplotlib.pyplot as plt
import numpy as np

app = Application()

api_base_url = app.settings.seismic_api.base_url
site = app.get_stations()

start_time = UTCDateTime(2018, 11, 20)
end_time =UTCDateTime(2018, 12, 1)

request_events = get_events_catalog(api_base_url, start_time, end_time)

for request_event in request_events:
    st = request_event.get_waveforms()
    cat = request_event.get_event()
    print(cat[0].preferred_origin())

    ot = cat[0].preferred_origin().time

    dist = np.linalg.norm(cat[0].preferred_origin().loc - cat[0].origins[0].loc)
    print(dist)

    plt.figure(1)
    plt.clf()
    st.distance_time_plot(cat[0], site)
    plt.title('automatic-distance: %d' % dist)
    plt.savefig('plots/' + str(ot) + '_automatic.pdf')

    cat[0].preferred_origin_id = cat[0].origins[0].resource_id.id

    plt.figure(2)
    plt.clf()
    st.distance_time_plot(cat[0], site)
    plt.title('manually-distance: %d' % dist)
    plt.savefig('plots/' + str(ot) + '_manual.pdf')
