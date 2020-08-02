from spp.clients import api_client
from glob import glob
import numpy as np
from importlib import reload
from spp.utils.application import Application
reload(api_client)

app = Application()
api_base_url = app.settings.seismic_api.base_url

with open('bad_files.txt', 'w') as ofile:
    for event_file in np.sort(glob('/Volumes/KINGSTON/data2/*.xml')):
        print(event_file)
        try:
            mseed_file = event_file.replace('xml', 'mseed')
            data, files = api_client.build_request_data_from_files(None,
                                                                   event_file,
                                                                   mseed_file,
                                                                   None)

            api_client.post_event_data(api_base_url, data, files)
        except:
            ofile.write(event_file)

