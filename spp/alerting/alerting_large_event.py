from microquake.core.settings import settings
from spp.clients.api_client import get_catalog
from obspy.core import UTCDateTime
import numpy as np
import bisect
from microquake.helpers.logging import logger
from microquake.helpers.time import get_time_zone
from pytz import utc
from datetime import datetime
from urllib.parse import urlencode, quote

from spp.alerting.alerting import AlertMessage

from spp.alerting.alert_db_helpers import (create_postgres_session,
                                           AlarmingState)
from sqlalchemy import desc

api_base_url = settings.get('API_BASE_URL')
local_tz = get_time_zone()

__alerting_type__ = 'large_event'


def alert_large_event(level_thresholds, severities, scanning_period_s=3600,
                      test=False):

    end_time = UTCDateTime.now()
    start_time = end_time - scanning_period_s

    res = get_catalog(api_base_url, start_time, end_time,
                      event_type='seismic event', status='accepted')

    session, engine = create_postgres_session()
    obj = session.query(AlarmingState).filter(
        AlarmingState.alert_type == __alerting_type__).order_by(desc(
           'time')).first()

    if test:
        logger.info('testing mode on...sending a test alert')
        message_core = """
    This is a test. 
            """
        am = AlertMessage(ms_host, ms_port, ms_username, ms_password,
                          ms_sender, ms_recipients)
        alert_level = 'test'
        am.send_message(alert_level, __alerting_type__, message_core)
        return

    if not res:
        logger.info(f'No event in the last {scanning_period_s/3600:0.0f} hour')
        return

    logger.info(f'{len(res)} event in the last '
                f'{scanning_period_s / 3600:0.0f} hour')

    indices = np.argsort(level_thresholds)
    level_thresholds = np.array(level_thresholds)[indices]
    severities = np.array(severities)[indices]

    for re in res:
        i = bisect.bisect_right(level_thresholds, re.magnitude)
        if i != 0:
            send_message = True
            previous_event = session.query(AlarmingState).filter(
                AlarmingState.supplemental_info ==
                re.event_resource_id).first()

            if previous_event is not None:
                logger.info('event already reported')
                # should probably check whether the event has been
                # reprocessed. It the event has been reprocessed
                # then send a notification update
                continue

            alert_level = i - 1
            severity = severities[alert_level]
            time_utc = re.time_utc.datetime.replace(tzinfo=utc)
            time_local = time_utc.astimezone(local_tz)
            logger.info(f'{severity} event occurred at {re.time_utc}')
            insertion_time = re.insertion_timestamp.datetime.replace(
                tzinfo=utc)
            insertion_time = insertion_time.astimezone(local_tz)

            message_core = """
A event of severity level {severity_level} occurred at {local_time} local time, {utc_time} UTC time.

The event was inserted in the database at {insertion_time} local time.

Event info
Moment Magnitude: {mw}
event time (local): {local_time}
event time (utc): {utc_time}
x: {x}
y: {y}
z: {z}

            """.format(severity_level=alert_level, local_time=time_local, utc_time=time_utc,
                       insertion_time=insertion_time, mw=re.magnitude, x=re.x, y=re.y, z=re.z)

        alert = AlarmingState()

        alert.time = datetime.utcnow()
        alert.alert_type = __alerting_type__
        alert.alert_level = alert_level
        alert.supplemental_info = re.event_resource_id

        session.add(alert)
        session.commit()

        am = AlertMessage(ms_host, ms_port, ms_username, ms_password,
                          ms_sender, ms_recipients)

        url_dict = {'site': 1,
                    'network': 1,
                    'status': 'accepted',
                    'time_range': 3}

        url_safe_event_id = quote(re.event_resource_id, safe='')
        url_params = urlencode(url_dict)

        wf_ui_url = f'{base_url_wf_ui}/events/{url_safe_event_id}?{url_params}'

        am.send_message(alert_level, __alerting_type__, message_core,
                        link_waveform_ui=wf_ui_url,
                        link_3d_ui=base_url_3d_ui)


if __name__ == "__main__":

    ms_host = settings.get('MAIL_SERVER_HOST')
    ms_port = settings.get('MAIL_SERVER_PORT')
    ms_username = settings.get('MAIL_SERVER_LOGIN')
    ms_password = settings.get('MAIL_SERVER_PASSWORD')
    ms_recipients = settings.get('ALERT_RECIPIENTS')
    ms_sender = settings.get('ALERT_SENDER')

    logger.info('starting event size alerting module')

    thresholds = settings.get('ALERT_LARGE_EVENT_THRESHOLDS')
    event_severities = settings.get('ALERT_LARGE_EVENT_SEVERITY_LEVELS')
    scanning_period = settings.get('ALERT_SCANNING_PERIOD_SECOND')

    base_url_wf_ui = settings.get('WAVEFORM_UI_BASE_URL')
    base_url_3d_ui = settings.get('3D_UI_BASE_URL')

    if scanning_period is None:
        scanning_period = 3600

    ths = []
    # converting text to int
    for item in thresholds:
        ths.append(float(item))

    ths = np.array(ths)
    event_severities = np.array(event_severities)

    alert_large_event(ths, event_severities,
                      scanning_period_s=scanning_period)






