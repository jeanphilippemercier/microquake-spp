from spp.utils.application import Application
from spp.utils.seismic_client import get_event_by_id

__module_name__ = 'initializer'

app = Application(module_name=__module_name__, processing_flow='automatic',
                  init_processing_flow=True)
app.init_module()

logger = app.get_logger('data_connector', 'data_connector.log')

event_id = "smi:local/8f0f1cbd-2f81-4050-8c62-fd72241f6752"  # 2018-07-06 event

settings = app.settings
api_base_url = settings.seismic_api.base_url
request = get_event_by_id(api_base_url, event_id)
if request is None:
    logger.error("seismic api returned None!")
    exit(0)
cat = request.get_event()
st  = request.get_waveforms()

for tr in st:
    if tr.stats.station in app.settings.sensors.black_list:
        st.remove(tr)
    else:
        print(tr.get_id())

app.send_message(cat, st)

