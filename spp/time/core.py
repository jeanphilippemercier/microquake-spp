

def get_time_zone():
    """
    Read time config file and return a time zone compatible object
    Handling of time zone is essential for seismic system as UTC time
    is used in the as the default time zone
    :return: a time zone object
    """

    import yaml
    import os

    fname = os.path.join(os.environ['SPP_CONFIG'], 'time.yaml')
    params = yaml.load(open(fname))

    if params['type'] == "UTC_offset":
        from dateutil.tz import tzoffset
        offset = float(params['offset'])    # offset in hours
        tz_code = params['time_zone_code']  # code for the time zone
        tz = tzoffset(tz_code, offset * 3600)

    elif params['type']  == "time_zone":
        import pytz
        valid_time_zones = pytz.all_timezones
        if params['time_zone_code'] not in valid_time_zones:
            # raise an exception
            pass
        else:
            try:
                tz = pytz.timezone(params['time_zone_code'])
            except Exception as e:
                print(e)
                return
    return tz


def localize(datetime):
    from microquake.core import UTCDateTime
    from datetime import datetime as dt
    from pytz import utc
    if isinstance(datetime, UTCDateTime):
        return datetime.datetime.replace(tzinfo=utc).astimezone(get_time_zone())

    elif isinstance(datetime, dt):
        if datetime.tzinfo:
            return datetime.astimezone(get_time_zone())
        else:
            return datetime.replace(tzinfo=utc).astimezone(get_time_zone())


def convert_datetime_to_epoch_nanoseconds(datetime_str):
    from microquake.core import UTCDateTime
    import numpy as np
    dt_epoch = int(np.float64(UTCDateTime(datetime_str).timestamp) * 1e9)
    return dt_epoch


def convert_epoch_nanoseconds_to_utc_datetime_string(nanoseconds_epoch):
    from datetime import datetime
    return datetime.utcfromtimestamp(nanoseconds_epoch / 1e6 / 1000.0).strftime('%Y-%m-%d %H:%M:%S.%f')


def convert_epoch_nanoseconds_to_local_datetime_string(nanoseconds_epoch):
    from datetime import datetime
    return datetime.fromtimestamp(nanoseconds_epoch / 1e6 / 1000.0).strftime('%Y-%m-%d %H:%M:%S.%f')
