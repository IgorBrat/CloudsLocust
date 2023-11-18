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
from google.oauth2.id_token import fetch_id_token


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
function_endpoint = f"https://{os.getenv('ZONE')}-{args.project_id}.cloudfunctions.net/{os.getenv('FUNC_NAME')}"

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


def prepare_message(data):
    return {
        "messages": [
            {
                "data": data,
            }
        ]
    }


def publish_message(data):
    resp = 0
    try:
        data = json.dumps(data)
        message_bytes = data.encode('ascii')
        base64_bytes = base64.b64encode(message_bytes)
        message_encoded = base64_bytes.decode('ascii')
        resp = session.post(target_url, json=(prepare_message(message_encoded)))
        print(resp)
        print(resp.content)
        print(resp.request.body)
    except Exception:
        print('shit')
    return resp


@app.get("/check")
def check():
    check_body = {
        'address': target_url,
        'data': {
            "type": "TEMP",
            "value": curr_temp,
            "datetime": str(datetime.datetime.now()),
            "latitude": latitude1 + (random.random() - 0.5) * 1e-2,
            "longitude": longitude1 + (random.random() - 0.5) * 1e-2,
        }
    }
    check_body = json.dumps(check_body)
    return flask.Response(check_body, HTTPStatus.OK)


@app.get("/send/temperature")
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
                "value": curr_temp,
                "datetime": str(datetime.datetime.now()),
                "latitude": latitude1 + (random.random() - 0.5) * 1e-2,
                "longitude": longitude1 + (random.random() - 0.5) * 1e-2,
            }
            resps.append(publish_message(data).content.decode('utf-8'))
            time.sleep(delay_ms * 1e-3)
            curr_temp = generate_temperature(curr_temp)
        except Exception as e:
            print(f'Exception: {e}')
            return flask.Response(status=HTTPStatus.BAD_REQUEST)
    resp_to_return['request_body'] = resps
    print('horay')
    return flask.Response(json.dumps(resp_to_return), status=HTTPStatus.OK)


@app.get("/send/humidity")
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
                "value": curr_humidity,
                "datetime": str(datetime.datetime.now()),
                "latitude": latitude2 + (random.random() - 0.5) * 1e-2,
                "longitude": longitude2 + (random.random() - 0.5) * 1e-2,
            }
            resps.append(publish_message(data).content.decode('utf-8'))
            time.sleep(delay_ms * 1e-3)
            curr_humidity = generate_humidity(curr_humidity)
        except Exception as e:
            print(f'Exception: {e}')
            return flask.Response(status=HTTPStatus.BAD_REQUEST)
    resp_to_return['request_body'] = resps
    return flask.Response(json.dumps(resp_to_return), status=HTTPStatus.OK)


@app.get("/send/heartbeat")
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
                "latitude": latitude3 + (random.random() - 0.5) * 1e-2,
                "longitude": longitude3 + (random.random() - 0.5) * 1e-2,
            }
            resps.append(publish_message(data).content.decode('utf-8'))
            time.sleep(delay_ms * 1e-3)
            curr_heartbeat = generate_heartbeat(curr_heartbeat)
        except Exception as e:
            print(f'Exception: {e}')
            return flask.Response(status=HTTPStatus.BAD_REQUEST)
    resp_to_return['request_body'] = resps
    return flask.Response(json.dumps(resp_to_return), status=HTTPStatus.OK)

@app.get("/getData")
def get_db_data():
    auth_req = google.auth.transport.requests.Request(session)
    token = fetch_id_token(auth_req, target_url)
    print(token)
    # resp = session.request('GET', function_endpoint,
    #                        headers={"Authorization": f"Bearer {token}"})
    resp = requests.request('GET', function_endpoint, headers={"Authorization": f"Bearer {token}"})
    return flask.Response(resp.content, status=HTTPStatus.OK)


creds, _ = google.auth.load_credentials_from_file(r'./resources/creds.json',
                                                  scopes=['https://www.googleapis.com/auth/pubsub',
                                                          ])
print(creds)
session = AuthorizedSession(creds)
app.run(host='0.0.0.0')
