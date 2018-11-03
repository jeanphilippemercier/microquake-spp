#!flask/bin/python
from flask import Flask, send_file, jsonify
from flask_restful import Resource, Api, reqparse
from spp.utils import get_stations
import numpy as np
from spp.travel_time import get_travel_time_grid_raw, get_travel_time_grid
from io import BytesIO
from time import time
from flask_restful.inputs import boolean

app = Flask(__name__)
api = Api(app)

site = get_stations()

grids = []
mq_grids = []
stations = []
phases = []

start = time()
for station in site.stations():
    for phase in ['P', 'S']:
        data = get_travel_time_grid_raw(station.code, phase)
        mq_grid = get_travel_time_grid(station.code, phase)
        stations.append(int(station.code))
        phases.append(phase)
        grids.append(data)
        mq_grids.append(mq_grid)
end = time()
print(end - start)

stations = np.array(stations)
phases = np.array(phases)
grids = np.array(grids)


class TravelTimeGrid(Resource):
    def get(self):
        args = parser.parse_args()
        st_id = args['st_id']
        phase = args['phase']
        index = np.nonzero((stations == st_id) & (phases == phase))[0]
        print(index)
        return send_file(BytesIO(grids[index][0]), as_attachment=True,
                         attachment_filename='test.bin',
                         mimetype='application/octet-stream')

class GridHeader(Resource):
    pass
    # return jsonify(grid_header)

class TravelTimePoint(Resource):
    def get(self):
        args = parser.parse_args()
        st_id = args['st_id']
        phase = args['phase']
        x = args['x']
        y = args['y']
        z = args['z']
        grid_coordinates = args['grid_coordinates']
        print(st_id, phase, grid_coordinates)
        index = np.nonzero((stations == st_id) & (phases == phase))[0][0]
        print(index)
        mq_grid = mq_grids[index]
        tt = mq_grid.interpolate((x, y, z), grid_coordinate=grid_coordinates)
        print(tt[0])
        return jsonify({'value': tt[0]})

parser = reqparse.RequestParser()
parser.add_argument('st_id', type=int)
parser.add_argument('phase', type=str)
parser.add_argument('x', type=float)
parser.add_argument('y', type=float)
parser.add_argument('z', type=float)
parser.add_argument('grid_coordinates', type=boolean)
api.add_resource(TravelTimeGrid, '/travel_time/grid')
api.add_resource(TravelTimePoint, '/travel_time/time')
import resource
print(resource.getrusage(resource.RUSAGE_SELF))

if __name__ == '__main__':
    # grids = get_data()
    # site = get_stations()
    from spp.utils import get_data_connector_parameters, get_stations
    # from spp.travel_time import get_travel_time_grid
    from microquake.core.data.grid import read_grid
    app.run(debug=True)