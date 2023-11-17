import base64
import datetime
import random
from http import HTTPStatus
from geopy import Nominatim
import flask
import json
import argparse
import time
import google.auth
from google.auth.transport.requests import AuthorizedSession


def generate_temperature(last_temp, min_threshold=0, max_threshold=60):
    temp = last_temp + (random.random() - 0.5) * 0.1
    if temp > max_threshold:
        return max_threshold
    if temp < min_threshold:
        return min_threshold
    return round(temp, 3)


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
    data = json.dumps(data)
    message_bytes = data.encode('ascii')
    base64_bytes = base64.b64encode(message_bytes)
    message_encoded = base64_bytes.decode('ascii')
    resp = session.post(target_url, json=(prepare_message(message_encoded)))
    print(resp)
    print(resp.content)
    print(resp.request.body)
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


@app.get("/send/temp")
def send_messages():
    global curr_temp
    resp_to_return = {}
    resps = []
    for _ in range(2):
        try:
            data = {
                "type": "TEMP",
                "value": curr_temp,
                "datetime": str(datetime.datetime.now()),
                "latitude": latitude1 + (random.random() - 0.5) * 1e-2,
                "longitude": longitude1 + (random.random() - 0.5) * 1e-2,
            }

            resps.append(publish_message(data))
            time.sleep(20 * 1e-3)
            curr_temp = generate_temperature(curr_temp)
        except Exception as e:
            print(f'Exception: {e}')
            return flask.Response(status=HTTPStatus.BAD_REQUEST)
    resp_to_return['request_body'] = resps
    return flask.Response(json.dumps(resp_to_return), status=HTTPStatus.OK)


creds, _ = google.auth.load_credentials_from_file(r'./resources/creds.json',
                                                  scopes=['https://www.googleapis.com/auth/pubsub'])
print(creds)
session = AuthorizedSession(creds)
app.run(host='0.0.0.0')
