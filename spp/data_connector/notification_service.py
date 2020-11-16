import os
from flask import Flask, request, jsonify

app = Flask(__name__)

# This is basically ready for us to activate / test,
# we may want to know where our service can send a notification
# (HTTP request) to indicate that a new event was written to the database
# (as per last item in point 2 of my original list).
# This will essentially be an event name and a status (type of update).


@app.route('/new_event', methods=['POST'])
def post_event():
    print(request.method)
    if request.method == 'POST':
        # event_id = request.args.get('event_id')
        data = request.form
        print(request.form)
        return jsonify(isError=False,
                       message="Success",
                       statusCode=200,
                       data=data), 200


if __name__ == '__main__':
    app.run()


# def create_app(test_config=None):
#     # create and configure the app
#     app = Flask(__name__, instance_relative_config=True)
#     app.config.from_mapping(
#         SECRET_KEY='dev',
#         DATABASE=os.path.join(app.instance_path, 'flaskr.sqlite'),
#     )
#
#     if test_config is None:
#         # load the instance config, if it exists, when not testing
#         app.config.from_pyfile('config.py', silent=True)
#     else:
#         # load the test config if passed in
#         app.config.from_mapping(test_config)
#
#     # ensure the instance folder exists
#     try:
#         os.makedirs(app.instance_path)
#     except OSError:
#         pass
#
#     # a simple page that says hello
#     @app.route('/hello')
#     def hello():
#         return 'Hello, World!'
#
#     return app