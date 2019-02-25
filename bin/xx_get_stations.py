
from microquake.core.data.inventory import inv_station_list_to_dict
from spp.utils.application import Application

'''
    A simple script to show how stationXML inventory can be read
    in and manipulated
'''
def main():

    # reading application data
    app = Application()
    settings = app.settings
    inventory = app.get_inventory()
    #sensors = inventory.get_sensor_types()
    #for sensor in sorted(sensors):
        #print(sensor)

    for station in inventory.stations():
        print("sta:%s loc:%s" % (station.code, station.loc))
        for channel in station.channels:
            print("  cha:%s cos:%s azim:%.1f dip:%.1f" % (channel.code, channel.cosines, channel.azimuth, channel.dip))

    station = inventory.select(net_code='OT', sta_code='17')
    print(station.code, station.loc)
    channel = inventory.select(net_code='OT', sta_code='17', cha_code='z')
    print(channel.response)
    channel.response.plot(min_freq=1)

    sta_codes = inventory.get_sta_codes(unique=False)
    print(sta_codes)
    stations = inventory.sort_by_motion(motion='acceleration')
    for sta in stations:
        print(sta.code, sta.sensor_id, sta.motion)

    exit()

    inventory = app.get_inventory()
    sta_meta_dict = inv_station_list_to_dict(inventory)
    for sta_code, sta in sta_meta_dict.items():
        # This station object is our microquake wrap of the obspy station class:
        #station = sta_meta_dict[sta_code]['station']
        station = sta['station']
        print("sta:%3s nchans:%d loc:<%.1f, %.1f, %.1f>" % \
              (station.code, station.total_number_of_channels, station.loc[0], station.loc[1], station.loc[2]))
        channels = station.channels
        for cha in channels:
            print("  cha:%s sensor_type:%s [motion:%s] cable_type:%s len:%d cosines:%s" % \
                  (cha.code, cha.sensor_type, cha.motion, cha.cable_type, int(cha.cable_length),cha.cosines))

    return

if __name__ == '__main__':

    main()
