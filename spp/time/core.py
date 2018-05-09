

def get_time_zone():
    """
    Read time config file and return a time zone compatible object
    Handling of time zone is essential for seismic system as UTC time
    is used in the as the default time zone
    :return: a time zone object
    """

    import json
    import os

    fname = os.path.join(os.environ['SPP_CONFIG'], 'time.json')
    params = json.load(open(fname))

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
                print e
                return
    return tz