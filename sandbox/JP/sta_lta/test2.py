from microquake.core.settings import settings
import numpy as np
from obspy.core import UTCDateTime
from spp.clients.ims import web_client
from microquake.db.connectors import RedisQueue
# from microquake.processors import event_detection
from obspy.signal.trigger import recursive_sta_lta, trigger_onset


def detect(trace):
    sta_lta_on = settings.get('event_detection').sta_lta_on_value
    sta_lta_off = settings.get('event_detection').sta_lta_off_value
    sta_length_second = settings.get('event_detection').sta_length_second
    lta_length_second = settings.get('event_detection').lta_length_second
    # onset_threshold = settings.get('event_detection').onset_threshold
    # max_trigger_length_second = settings.get(
    #     'event_detection').max_trigger_length_second
    # ims_base_url = settings.get('ims_base_url')
    inventory = settings.inventory
    # network_code = settings.get('network_code')

    tr = trace

    tr = tr.detrend('demean').detrend('linear')
    sensor_id = tr.stats.station

    sensor = inventory.select(sensor_id)

    poles = np.abs(sensor[0].response.get_paz().poles)
    low_bp_freq = np.min(poles) / (2 * np.pi)
    tr = tr.filter('highpass', freq=low_bp_freq)

    df = tr.stats.sampling_rate
    cft = recursive_sta_lta(tr.data, int(sta_length_second * df),
                            int(lta_length_second * df))
    on_off = trigger_onset(cft, sta_lta_on, sta_lta_off)
    sr = tr.stats.sampling_rate
    st = tr.stats.starttime
    on = []
    off = []
    for o_f in on_off:
        on.append(st + o_f[0] / sr)
        off.append(st + o_f[1] / sr)

    return on, off

    # edp = event_detection.Processor()
    # on, off = edp.process(trace=trace)
    # return on, off


network = settings.get('network_code')
inventory = settings.inventory

etime = UTCDateTime(2020, 6, 3, 17, 51, 19)

starttime = etime - 10
endtime  = etime + 10
site_id = '146'
rq = RedisQueue('test')
base_url = settings.get('ims_base_url')

# job = rq.submit_task(web_client.get_continuous, base_url, starttime, endtime, [site_id], '')
#
st = job.result
st = st.detrend('demean').detrend('linear')
tr = st.composite()[0]
