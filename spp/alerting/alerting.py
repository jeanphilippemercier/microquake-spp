import smtplib
from os import environ
from microquake.core.settings import settings
from microquake.clients.api_client import get_catalog
import requests
from dateutil.parser import parse
from datetime import datetime, timedelta
from pytz import utc
# from spp.alerting.alert_db_helpers import (create_postgres_session,
#                                            AlarmingState)

# from .alert_db_helpers import (create_postgres_session, AlarmingState)
from .alert_db_helpers import (create_postgres_session, AlarmingState)
import pandas as pd
from sklearn.cluster import MeanShift
from microquake.helpers.logging import logger
import numpy as np
from sqlalchemy import desc

# ms_host = settings.get('MAIL_SERVER_HOST')
# ms_port = settings.get('MAIL_SERVER_PORT')
# ms_username = settings.get('MAIL_SERVER_LOGIN')
# ms_password = settings.get('MAIL_SERVER_PASSWORD')
# ms_recipients = settings.get('ALERT_RECIPIENTS')

ms_host = environ['SPP_MAIL_SERVER_HOST']
ms_port = environ['SPP_MAIL_SERVER_PORT']
ms_username = environ['SPP_MAIL_SERVER_LOGIN']
ms_password = environ['SPP_MAIL_SERVER_PASSWORD']
ms_recipients = environ['SPP_ALERT_RECIPIENTS']

ms_sender = 'Seismic System Automatic Alerting Service <alerts@microquake.org>'


class AlertMessage():
    def __init__(self, host, port, username, password, sender, recipients):
        self.host = host
        self.port = port
        self.username = username
        self.password = password
        self.sender = sender
        self.recipients = recipients
#         self.message_core = message_core
#         self.message = """From: {sender}
# To: Jean-Philippe Mercier <jpmercier01@gmail.com>
# Subject: MICROQUAKE ALERT / SEVERITY LEVEL {alert_level} / {alert_topic}
#
# {message}
#
# {link_waveform_ui}
# {link_3d_ui}
#         """.format(sender=sender, recipients=recipients,
#                    alert_level=alert_level, alert_topic=alert_topic,
#                    message=message_core, link_waveform_ui=link_waveform_ui,
#                    link_3d_ui=link_3d_ui)

    def send_message(self, alert_level, alert_topic, message,
                     link_waveform_ui="", link_3d_ui=""):

        # sender = 'alerts@microquake.org'
        # recipients = 'jpmercier01@gmail.com'

        self.message = """From: {sender}
To: Jean-Philippe Mercier <jpmercier01@gmail.com>
Subject: MICROQUAKE ALERT / SEVERITY LEVEL {alert_level} / {alert_topic}

{message}

{link_waveform_ui}
{link_3d_ui}
                """.format(sender=self.sender, recipients=self.recipients,
                           alert_level=alert_level, alert_topic=alert_topic,
                           message=message, link_waveform_ui=link_waveform_ui,
                           link_3d_ui=link_3d_ui)

        try:
            server = smtplib.SMTP_SSL(self.host, self.port)
            server.ehlo()
            server.login(self.username, self.password)
            server.sendmail(self.sender, self.recipients, self.message)
            server.close()
            logger.info("Successfully sent email")
        except Exception as e:
            logger.error("Error: unable to send email")
            logger.error(e)


# types of alerts
# 1 - Connector is not working
# 2 - Number of sensor offline
# 3 - Large events
# 4 - Event rates
# 5 - API is not reachable or down

# host = 'mail.gandi.net'
# port = 465
# username = 'alerts@microquake.org'
# password = 'mfY-pqu-dXD-9MZ'

# message = """From: Seismic Alerts <alerts@microquake.org>
# To: Jean-Philippe Mercier <jpmercier01@gmail.com>
# Subject: MICROQUAKE ALERT <ALERT_LEVEL>: <ALERT_TOPIC>
#
# <MESSAGE>
#
# <LINKS_WAVEFORM_UI>
# <LINKS_3D_UI>
# """

api_base_url = settings.get('api_base_url')

if api_base_url[-1] != '/':
    api_base_url = '/'

# send_message(message)

# Connector check threshold

magnitude_threshold = settings.get('ALERT_EVENT_RATE_THRESHOLD')
event_rate_level_1 = settings.get('alert_event_rate_level_1')
event_rate_level_2 = settings.get('alert_event_rate_level_2')
event_rate_level_3 = settings.get('alert_event_rate_level_3')


def seismic_activity_rate(end_time=datetime.now(),
                          window_size_hours=1,
                          mean_shift_bandwidth=100):
    """
    Find seismic event cluster and calculate the seismic event rate within
    those cluster. Returns information about the cluster where activity rate
    exceeds the predefined threshold. The clustering is performed using the
    MeanShift algorithm. The mean shift algorithm is parameterized by it
    bandwidth. The bandwidth is basically the size of the kernel
    :return:
    """
    start_time = end_time - timedelta(hours=window_size_hours)

    res = get_catalog(api_base_url, start_time, end_time,
                      event_type='seismic event', status='accepted')

    if len(res) < event_rate_level_1:
        logger.info(f'The number of events {len(res)} within the time window '
                    f'is smaller than the threshold for level 1 '
                    f'({event_rate_level_1}). Exiting!')
        return 1

    x = []
    y = []
    z = []
    mag = []
    ev_time = []
    for re in res:
        # if re.magnitude > magnitude_threshold:
            x.append(re.x)
            y.append(re.y)
            z.append(re.z)
            mag.append(re.magnitude)
            ev_time.append(re.time_utc)

    if len(x) < event_rate_level_1:
        logger.info(f'The number of events {len(res)} within the time window '
                    f'is smaller than the threshold for level 1 '
                    f'({event_rate_level_1}). Exiting!')
        return 1

    df = pd.DataFrame({'x': x, 'y': y, 'z': z})

    # bandwidth = settings.get('clustering_mean_shift_bandwidth_meter')
    bandwidth = mean_shift_bandwidth

    logger.info(f'Clustering using the Mean Shift algorithm using a '
                f'bandwidth of {bandwidth} meter.')

    # estimate_bandwidth(df, quantile=0.2)
    ms = MeanShift(bandwidth=bandwidth )
    ms.fit(df)
    labels = ms.predict(df)
    centroids = ms.cluster_centers_

    logger.info(f'{len(np.unique(labels))} cluster found')

    df['label'] = labels
    df['magnitude'] = mag
    df['ev_time'] = ev_time

    df_label = df.groupby('label')

    clusters = []
    for i, key in enumerate(df_label.groups.keys()):
        group = df_label.groups[key]
        if len(group) < event_rate_level_1:
            continue

        cluster = {'number_of_events' : len(group),
                   'activity_rate_hours': len(group) / window_size_hours,
                   'normalized_number_of_events': 0,
                   'centroids': centroids[i],
                   'max_magnitude': np.max(df.loc[group])}

        clusters.append(cluster)

    return clusters, df, centroids


def alert_heartbeat(test_mode=False):
    """
    Check if the heartbeat signal was received from the data connector. If
    not an alert is raised. The alert is periodically raised again if the
    :param test_mode:
    :return:
    """
    alert_topic = 'Data Connector'

    alert = AlarmingState()

    session, engine = create_postgres_session()
    obj = session.query(AlarmingState).filter(
                        AlarmingState.alert_type=='connector').order_by(desc(
                        'time')).first()

    last_alert = session.query(AlarmingState).filter(
        AlarmingState.alert_type == 'connector').filter(
        AlarmingState.alert_sent).order_by(desc('time')).first()

    current_alert_level = 0

    if obj is not None:
        current_alert_level = obj.alert_level

    if last_alert is not None:
        last_alert_time = last_alert.time
        delay_tmp = last_alert_time - obj.time
        last_alert_delay_minute = delay_tmp.total_second() / 60
    else:
        last_alert_delay_minute = 0

    AlarmingState.time = datetime.utcnow().replace(tzinfo=utc)

    alert_connector_1 = settings.get('alert_connector_level_1_min')
    alert_connector_2 = settings.get('alert_connector_level_2_min')

    heartbeat = requests.get(f'{api_base_url}'
                             f'inventory/heartbeat/event_connector' )
    last_hb_time = parse(eval(heartbeat.content.decode())['last_heard'])
    last_hb_time = last_hb_time.replace(tzinfo=utc)
    last_hb_delay = datetime.utcnow().replace(tzinfo=utc) - last_hb_time
    last_hb_delay_minute = last_hb_delay.total_seconds() / 60

    alert_recurrence_time = settings.get('ALERT_CONNECTOR_RECURRENCE_TIME_HOUR')

    message_core = """
    
LEVEL {alert_level} ALERT!

The data connector on-premise has been down for {minute} minutes.

    """

    send_message = False

    if test_mode:
        alert_level = 'test'

        message_core = """
This is a test for the connector alert module, this message will be 
sent periodically to ensure the alerting module is active and working. 
        
The health status of the connector was last checked {minute:0.2f} minutes ago

The current alert level is {alert_level}
        
""".format(minute=last_hb_delay_minute, alert_level=current_alert_level)

        am = AlertMessage(ms_host, ms_port, ms_username, ms_password,
                          ms_sender, ms_recipients, alert_level,
                          alert_topic, message_core)

        am.send_message()

        return

    elif last_hb_delay_minute > alert_connector_2:

        alert_level = 2

        if current_alert_level != 2:

            send_message = True

        elif last_alert_delay_minute > alert_recurrence_time:

            send_message = True

    elif last_hb_delay_minute > alert_connector_1:

        alert_level = 1

        if current_alert_level != 1:
            message_core.format(minute=last_hb_delay_minute,
                                alert_level=alert_level)

            send_message = True

    else:
        alert_level = 0

    alert.time = datetime.utcnow()
    alert.alert_type = 'connector'
    alert.alert_level = alert_level

    session.add(alert)
    session.commit()

    if send_message:
        message_core = message_core.format(minute=int(last_hb_delay_minute),
                                           alert_level=alert_level)
        am = AlertMessage(ms_host, ms_port, ms_username, ms_password,
                          ms_sender, ms_recipients, alert_level,
                          alert_topic, message_core)

        am.send_message()