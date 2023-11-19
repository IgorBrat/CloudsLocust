import base64
import datetime
import os
import random
from http import HTTPStatus

import requests
from geopy import Nominatim
import flask
import json
import argparse
import time
import google.auth
from google.auth.transport.requests import AuthorizedSession
from google.oauth2 import id_token


def generate_temperature(last_temp, min_threshold=0, max_threshold=60):
    temp = last_temp + (random.random() - 0.5) * 0.1
    if temp > max_threshold:
        return max_threshold
    if temp < min_threshold:
        return min_threshold
    return round(temp, 3)


def generate_humidity(last_humidity, min_threshold=0, max_threshold=100):
    humidity = last_humidity + (random.random() - 0.5) * 0.5
    if humidity > max_threshold:
        return max_threshold
    if humidity < min_threshold:
        return min_threshold
    return humidity


def generate_heartbeat(last_heartbeat, min_threshold=60, max_threshold=180):
    heartbeat = last_heartbeat + random.randint(-2, 2)
    if heartbeat > max_threshold:
        return max_threshold
    if heartbeat < min_threshold:
        return min_threshold
    return heartbeat


# Auth

credentials_path = r"./resources/creds.json"

parser = argparse.ArgumentParser(description='Humidity measure device')
parser.add_argument('--project_id', type=str, help='Receiver gcp project id')
parser.add_argument('--topic_id', type=str, help='Receiver gcp topic id')
args = parser.parse_args()

if not args.project_id:
    raise ValueError('Specify project id')
if not args.topic_id:
    raise ValueError('Specify topic id')

target_url = f"https://pubsub.googleapis.com/v1/projects/{args.project_id}/topics/{args.topic_id}:publish"
function_endpoint = f"https://{os.getenv('REGION')}-{args.project_id}.cloudfunctions.net/{os.getenv('FUNC_NAME')}"

# Location
loc = Nominatim(user_agent="GetLoc")

getLoc1 = loc.geocode("New York USA")
latitude1 = getLoc1.latitude
longitude1 = getLoc1.longitude
getLoc2 = loc.geocode("Brussel Belgium")
latitude2 = getLoc2.latitude
longitude2 = getLoc2.longitude
getLoc3 = loc.geocode("Paris France")
latitude3 = getLoc3.latitude
longitude3 = getLoc3.longitude

curr_temp = 25
curr_humidity = 60
curr_heartbeat = 110

# Web Server
app = flask.Flask(__name__)


def prepare_message(data, value_type):
    return {
        "messages": [
            {
                "data": data,
                "attributes": {
                    "value_type": value_type,
                }
            }
        ]
    }


def publish_message(data, value_type=None):
    resp = 0
    try:
        data = json.dumps(data)
        message_bytes = data.encode('ascii')
        base64_bytes = base64.b64encode(message_bytes)
        message_encoded = base64_bytes.decode('ascii')
        resp = session.post(target_url, json=(prepare_message(message_encoded, value_type)))
    except Exception as e:
        print(f'Exception when publishing: {e}')
    return resp

@app.post("/temperature1")
def send_temperature1():
    num = int(flask.request.args.get('num'))
    delay_ms = int(flask.request.args.get('delay'))
    global curr_temp
    resp_to_return = {}
    resps = []
    for _ in range(num):
        try:
            data = {
                "type": "TEMP",
                "value": round(curr_temp, 3),
                "datetime": str(datetime.datetime.now()),
                "latitude": round(latitude1 + (random.random() - 0.5) * 1e-2, 5),
                "longitude": round(longitude1 + (random.random() - 0.5) * 1e-2, 5),
            }
            resps.append(publish_message(data, "TEMP").content.decode('utf-8'))
            time.sleep(delay_ms * 1e-3)
            curr_temp = generate_temperature(curr_temp)
        except Exception as e:
            print(f'Exception: {e}')
            return flask.Response(status=HTTPStatus.BAD_REQUEST)
    resp_to_return['request_body'] = resps
    return flask.Response(json.dumps(resp_to_return), status=HTTPStatus.OK)

@app.post("/temperature")
def send_temperature():
    num = int(flask.request.args.get('num'))
    delay_ms = int(flask.request.args.get('delay'))
    global curr_temp
    resp_to_return = {}
    resps = []
    for _ in range(num):
        try:
            data = {
                "type": "TEMP",
                "value": round(curr_temp, 3),
                "datetime": str(datetime.datetime.now()),
                "latitude": round(latitude1 + (random.random() - 0.5) * 1e-2, 5),
                "longitude": round(longitude1 + (random.random() - 0.5) * 1e-2, 5),
            }
            resps.append(publish_message(data).content.decode('utf-8'))
            time.sleep(delay_ms * 1e-3)
            curr_temp = generate_temperature(curr_temp)
        except Exception as e:
            print(f'Exception: {e}')
            return flask.Response(status=HTTPStatus.BAD_REQUEST)
    resp_to_return['request_body'] = resps
    return flask.Response(json.dumps(resp_to_return), status=HTTPStatus.OK)


@app.post("/humidity")
def send_humidity():
    num = int(flask.request.args.get('num'))
    delay_ms = int(flask.request.args.get('delay'))
    global curr_humidity
    resp_to_return = {}
    resps = []
    for _ in range(num):
        try:
            data = {
                "type": "HUM",
                "value": round(curr_humidity, 3),
                "datetime": str(datetime.datetime.now()),
                "latitude": round(latitude2 + (random.random() - 0.5) * 1e-2, 5),
                "longitude": round(longitude2 + (random.random() - 0.5) * 1e-2, 5),
            }
            resps.append(publish_message(data).content.decode('utf-8'))
            time.sleep(delay_ms * 1e-3)
            curr_humidity = generate_humidity(curr_humidity)
        except Exception as e:
            print(f'Exception: {e}')
            return flask.Response(status=HTTPStatus.BAD_REQUEST)
    resp_to_return['request_body'] = resps
    return flask.Response(json.dumps(resp_to_return), status=HTTPStatus.OK)


@app.post("/heartbeat")
def send_heartbeat():
    num = int(flask.request.args.get('num'))
    delay_ms = int(flask.request.args.get('delay'))
    global curr_heartbeat
    resp_to_return = {}
    resps = []
    for _ in range(num):
        try:
            data = {
                "type": "HB",
                "value": curr_heartbeat,
                "datetime": str(datetime.datetime.now()),
                "latitude": round(latitude3 + (random.random() - 0.5) * 1e-2, 5),
                "longitude": round(longitude3 + (random.random() - 0.5) * 1e-2, 5),
            }
            resps.append(publish_message(data).content.decode('utf-8'))
            time.sleep(delay_ms * 1e-3)
            curr_heartbeat = generate_heartbeat(curr_heartbeat)
        except Exception as e:
            print(f'Exception: {e}')
            return flask.Response(status=HTTPStatus.BAD_REQUEST)
    resp_to_return['request_body'] = resps
    return flask.Response(json.dumps(resp_to_return), status=HTTPStatus.OK)


@app.post("/junk")
def send_junk():
    num = int(flask.request.args.get('num'))
    delay_ms = int(flask.request.args.get('delay'))
    resp_to_return = {}
    resps = []
    for _ in range(num):
        try:
            data = {
                "type": "GAS",
                "val": random.randint(-100, 100),
                "datetime": str(datetime.datetime.now()),
                "latitude": random.randint(-50, 50),
            }
            resps.append(publish_message(data).content.decode('utf-8'))
            time.sleep(delay_ms * 1e-3)
        except Exception as e:
            print(f'Exception: {e}')
            return flask.Response(str(e), status=HTTPStatus.BAD_REQUEST)
    resp_to_return['request_body'] = resps
    return flask.Response(json.dumps(resp_to_return), status=HTTPStatus.OK)


@app.get("/get_data")
def get_all_db_data():
    val_type = flask.request.args.get('type')
    endpoint = function_endpoint
    if not val_type:
        pass
    elif val_type in ['TEMP', 'HUMIDITY', 'HB']:
        endpoint += f'?type={val_type}'
    else:
        return flask.Response('No such device type in DB', HTTPStatus.NOT_FOUND)
    token = id_token.fetch_id_token(auth_req, function_endpoint)
    resp = requests.request('GET', endpoint, headers={"Authorization": f"Bearer {token}"})
    data = json.loads(resp.content.decode('utf-8'))['response']
    return flask.render_template('visualise_data.html',
                                 data=data,
                                 keys=['type', 'value', 'datetime', 'latitude', 'longitude'])


creds, _ = google.auth.load_credentials_from_file(credentials_path,
                                                  scopes=['https://www.googleapis.com/auth/pubsub'])
session = AuthorizedSession(creds)
auth_req = google.auth.transport.requests.Request()
app.run(host='0.0.0.0')
