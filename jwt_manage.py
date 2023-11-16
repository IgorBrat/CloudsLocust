from google.auth import crypt
from google.auth import jwt
import requests
import time

def auth_publish(jwt, data, url):
    headers = {
        'Authorization': f'Bearer {jwt.decode("utf-8")}',
        'content-type': 'application/json'
    }
    json_data = {
        "messages": [
            {
                "data": data,
                "ordering_key": "first order",
            }
        ]
    }
    response = requests.post(url, headers=headers, json=json_data)
    print(response.status_code, response.content)
    response.raise_for_status()


def generate_jwt(service_account_keyfile, service_account_email, audience, expiration=3600):
    req_time = int(time.time())
    payload = {
        'iat': req_time,
        "exp": req_time + expiration,
        'iss': service_account_email, # issuer
        'aud': audience,
        'sub': service_account_email,
        'email': service_account_email
    }
    signer = crypt.RSASigner.from_service_account_file(service_account_keyfile)
    jwt_token = jwt.encode(signer, payload)
    return jwt_token
