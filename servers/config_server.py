#!flask/bin/python
from flask import Flask, jsonify
from spp.utils import get_data_connector_parameters
# from spp.travel_time import get_travel_time_grid

app = Flask(__name__)

# @app.route('/')
# def index():
#     return "Hello, World!"



# tasks = [
#     {
#         'id': 1,
#         'title': u'Buy groceries',
#         'description': u'Milk, Cheese, Pizza, Fruit, Tylenol',
#         'done': False
#     },
#     {
#         'id': 2,
#         'title': u'Learn Python',
#         'description': u'Need to find a good Python tutorial on the web',
#         'done': False
#     }
# ]


@app.route('/todo/api/v1.0/data_connector_params', methods=['GET'])
def get_tasks():
    data_connect_params = get_data_connector_parameters()
    return jsonify({'data_connector': data_connect_params})

if __name__ == '__main__':
    app.run(debug=True)

