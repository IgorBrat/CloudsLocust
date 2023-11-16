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


parser = argparse.ArgumentParser(description='Temperature measure device')

# Parse arguments
parser.add_argument('--num', type=int, help='Number of messages to send')
parser.add_argument('--delay_ms', type=int, default=20, help='Sending data delay')
parser.add_argument('--project_id', type=str, help='Receiver gcp project id')
parser.add_argument('--topic_id', type=str, help='Receiver gcp topic id')

args = parser.parse_args()
if not args.num:
    raise ValueError('Number of messages can`t be None or zero')
if args.delay_ms <= 0:
    raise ValueError('Delay can`t be negative of zero')
if not args.project_id:
    raise ValueError('Specify project id')
if not args.topic_id:
    raise ValueError('Specify topic id')

# Auth
credentials_path = r"./resources/creds.json"

target_url = f"https://pubsub.googleapis.com/v1/projects/{args.project_id}/topics/{args.topic_id}:publish"

# Location
loc = Nominatim(user_agent="GetLoc")

getLoc = loc.geocode("New York USA")
latitude = getLoc.latitude
longitude = getLoc.longitude

curr_temp = 25

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


@app.get("/check")
def check():
    check_body = {
        'address': target_url,
        'data': {
            "type": "TEMP",
            "value": curr_temp,
            "datetime": str(datetime.datetime.now()),
            "latitude": latitude + (random.random() - 0.5) * 1e-2,
            "longitude": longitude + (random.random() - 0.5) * 1e-2,
        }
    }
    check_body = json.dumps(check_body)
    return flask.Response(check_body, HTTPStatus.OK)


@app.get("/post")
def send_messages():
    curr_heartbeat = 110
    for _ in range(args.num):
        try:
            data = {
                "type": "HB",
                "value": curr_heartbeat,
                "datetime": str(datetime.datetime.now()),
                "latitude": latitude + (random.random() - 0.5) * 1e-2,
                "longitude": longitude + (random.random() - 0.5) * 1e-2,
            }

            data = json.dumps(data)
            message_bytes = data.encode('ascii')
            base64_bytes = base64.b64encode(message_bytes)
            message_encoded = base64_bytes.decode('ascii')
            resp = session.post(target_url, data=prepare_message(message_encoded))
            print(resp)
            time.sleep(args.delay_ms * 1e-3)
        except Exception as e:
            print(f'Exception: {e}')
    return flask.Response(status=HTTPStatus.OK)


creds, _ = google.auth.load_credentials_from_file(r'./resources/creds.json',
                                                  scopes=['https://www.googleapis.com/auth/pubsub'])
print(creds)
session = AuthorizedSession(creds)
app.run(host='0.0.0.0')
