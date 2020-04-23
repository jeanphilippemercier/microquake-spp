from microquake.core.settings import settings
import requests
from dateutil.parser import parse
from datetime import datetime
from pytz import utc
from os import environ

from spp.alerting.alert_db_helpers import (create_postgres_session,
                                         AlarmingState)
from spp.alerting.alerting import AlertMessage
from sqlalchemy import desc

api_base_url = settings.get('api_base_url')

if api_base_url[-1] != '/':
    api_base_url = '/'


def alert_heartbeat(alert_connector_level_1, alert_connector_level_2,
                    alert_recurrence_time, test_mode=False):
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
        AlarmingState.alert_type == 'connector').order_by(desc(
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

    heartbeat = requests.get(f'{api_base_url}'
                             f'inventory/heartbeat/event_connector')
    last_hb_time = parse(eval(heartbeat.content.decode())['last_heard'])
    last_hb_time = last_hb_time.replace(tzinfo=utc)
    last_hb_delay = datetime.utcnow().replace(tzinfo=utc) - last_hb_time
    last_hb_delay_minute = last_hb_delay.total_seconds() / 60

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

    elif last_hb_delay_minute > alert_connector_level_2:

        alert_level = 2

        if current_alert_level != 2:

            send_message = True

        elif last_alert_delay_minute > alert_recurrence_time:

            send_message = True

    elif last_hb_delay_minute > alert_connector_level_1:

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

if __name__ == "__main__":

    ms_host = settings.get('MAIL_SERVER_HOST')
    ms_port = settings.get('MAIL_SERVER_PORT')
    ms_username = settings.get('MAIL_SERVER_LOGIN')
    ms_password = settings.get('MAIL_SERVER_PASSWORD')
    ms_recipients = settings.get('ALERT_RECIPIENTS')
    ms_sender = settings.get('ALERT_SENDER')

    alert_connector_level_1 = settings.get('alert_connector_level_1_min')
    alert_connector_level_2 = settings.get('alert_connector_level_2_min')

    alert_recurrence_time = settings.get(
        'ALERT_CONNECTOR_RECURRENCE_TIME_HOUR')

    am = AlertMessage(ms_host, ms_port, ms_username, ms_password, ms_sender,
                      ms_recipients)

    alert_heartbeat(alert_connector_level_1, alert_connector_level_2,
                    alert_recurrence_time, am)