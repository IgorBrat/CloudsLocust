import datetime
import random
from geopy import Nominatim
import time
import os
import io
import json
import base64
import argparse
from jwt_manage import auth_publish, generate_jwt

parser = argparse.ArgumentParser(description='Heartbeat measure device')

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
credentials_path = "./resources/creds.json"
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = credentials_path
service_account_keyfile = credentials_path

with io.open(credentials_path, "r", encoding="utf-8") as json_file:
    data = json.loads(json_file.read())
    service_account_email = data['client_email']

audience = "https://pubsub.googleapis.com/google.pubsub.v1.Publisher"

url = f"https://pubsub.googleapis.com/v1/projects/{os.getenv('PROJECT_ID')}/topics/{os.getenv('TOPIC_ID')}:publish"


def generate_heartbeat(last_heartbeat, min_threshold=60, max_threshold=180):
    heartbeat = last_heartbeat + random.randint(-2, 2)
    if heartbeat > max_threshold:
        return max_threshold
    if heartbeat < min_threshold:
        return min_threshold
    return heartbeat


# Sign token
signed_jwt = generate_jwt(service_account_keyfile, service_account_email, audience)

# Location
loc = Nominatim(user_agent="GetLoc")

getLoc = loc.geocode("Brussel Belgium")
latitude = getLoc.latitude
longitude = getLoc.longitude

curr_heartbeat = 110

# Publish messages
for _ in range(args.num):
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
    encoded_element = base64_bytes.decode('ascii')
    auth_publish(signed_jwt, encoded_element, url)

    time.sleep(args.delay_ms * 1e-3)
    curr_heartbeat = generate_heartbeat(curr_heartbeat)
