def convert_datetime_to_epoch_nanoseconds(datetime_str):
    from obspy import UTCDateTime
    import numpy as np
    dt_epoch = int(np.float64(UTCDateTime(datetime_str).timestamp) * 1e9)
    return dt_epoch


def convert_epoch_nanoseconds_to_utc_datetime_string(nanoseconds_epoch):
    from datetime import datetime
    return datetime.utcfromtimestamp(nanoseconds_epoch / 1e6 /
                                     1000.0).strftime('%Y-%m-%d %H:%M:%S.%f')


def convert_epoch_nanoseconds_to_local_datetime_string(nanoseconds_epoch):
    from datetime import datetime
    return datetime.fromtimestamp(nanoseconds_epoch / 1e6 /
                                  1000.0).strftime('%Y-%m-%d %H:%M:%S.%f')
