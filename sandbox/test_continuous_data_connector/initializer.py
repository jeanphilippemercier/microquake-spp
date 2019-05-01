from datetime import datetime, timedelta
from spp.utils.application import Application
import faust


app = faust.App('event_detection', broker='broker:9092',
                value_serializer='raw')

spp_app = Application()

inventory = spp_app.get_inventory()
station_codes = [station.code for station in inventory.stations()]

offset = 2 * 60  # offset in second from now to set the starttime this will
# need to be in a config file

endtime = datetime.now()
starttime = endtime - timedelta(seconds=offset)

continuous_data = app.topic('continuous_data', value_type=str)
@app.agent(continuous_data)
async def data_connector(messages):
    async for message in messages:
        print(message)

@app.task(on_leader=True)
async def initialize():
    for station_code in station_codes:
        print('PUBLISHING ON LEADER!')
        message = '%s, %s, %s' % (starttime, endtime, station_code)
        data_connector.send(value=message)

# if __name__ == '__main__':
# initialize(starttime, endtime, station_codes)





# kproducer = Producer(
#             {"bootstrap.servers": app.settings.get('kafka').brokers}
#             )
# p = Producer({'bootstrap.servers': 'broker:9092'})