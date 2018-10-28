#!flask/bin/python
from flask import Flask, jsonify, send_file
from io import BytesIO
#
from spp.travel_time import get_travel_time_grid
from spp.utils import get_stations
import pickle
from base64 import b64encode
import numpy as np
from spp.travel_time import get_travel_time_grid_raw

app = Flask(__name__)

#grids = [get_travel_time_grid(station.code, 'P') for station in
site = get_stations()


grids = []

for station in site.stations()[:10]:
    for phase in ['P', 'S']:
        print(station.code, phase)
        bytes_io = get_travel_time_grid_raw(station.code, phase)
        print(bytes_io)
        grids.append(bytes_io)

    # grids.append(get_travel_time_grid(station.code, 'P').__dict__)
    # grids.append(get_travel_time_grid(station.code, 'S').__dict__)



    # print(station.code)
    # grids['grids'].append(get_travel_time_grid(station.code, 'P'))
    # grids['grids'].append(get_travel_time_grid(station.code, 'S'))



# @app.route('/')
# def index():
#     return "Hello, World!"


# tasks = [
#     {
#         'id': 1,
#         'title': u'Buy groceries',
#         'description': u'Milk, Cheese, Pizza, Fruit, Tylenol',
#         'done': o
#     },
#     {
#         'id': 2,
#         'title': u'Learn Python',
#         'description': u'Need to find a good Python tutorial on the web',
#         'done': False
#     }
# ]

@app.route('/todo/api/v1.0/grid/<int:st_id>', methods=['GET'])
def get_tasks(st_id):
    # return grids['grids'][0]
    # data_connect_params = get_data_connector_parameters()
    # return jsonify({'data_connector': grids[st_id]})
    # obj = BytesIO()
    # pickle.dump(grids[st_id], obj)
    # obj.seek(0)
    return send_file(grids[st_id], as_attachment=True,
                     attachment_filename='test.bin',
                     mimetype='application/octet-stream')

if __name__ == '__main__':
    # grids = get_data()
    # site = get_stations()
    from spp.utils import get_data_connector_parameters, get_stations
    # from spp.travel_time import get_travel_time_grid
    from microquake.core.data.grid import read_grid
    app.run(debug=False)