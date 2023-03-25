import flask

import main


app = flask.Flask(__name__)


@app.route("/test_auth")
def index():
    return main.test_auth(flask.request)
