import base64
import logging

import requests

from airflow.exceptions import AirflowException
from config.appconfig import app_config
from scripts.utils.tokens.save_access_token import save_access_token


def token_from_authorization_code(authorization_code: str):
    """Generate token using authorization code"""

    CLIENT_ID = app_config.get_spotify_client_id()
    CLIENT_SECRET = app_config.get_spotify_client_secret()
    REDIRECT_URI = app_config.get_spotify_redirect_uri()
    url = "https://accounts.spotify.com/api/token"

    credentials = "%s:%s" % (CLIENT_ID, CLIENT_SECRET)

    base64_encoded = base64.b64encode(credentials.encode()).decode()
    logging.info("Retrieving access tokens........")
    response = requests.post(
        url,
        data={
            "grant_type": "authorization_code",
            "code": authorization_code,
            "redirect_uri": REDIRECT_URI,
        },
        headers={"Authorization": "Basic " + base64_encoded},
    )
    logging.info("Access tokens retrieved........")
    response_json = response.json()
    access_token = response_json.get("access_token")

    return access_token


def get_access_token():
    """Read access token from file written by authorize_user task"""
    import os
    token_path = "/opt/data/token.txt"
    if os.path.exists(token_path):
        with open(token_path, "r") as f:
            token = f.read().strip()
            if token:
                return token
    logging.error("Authorization code is required")
    raise AirflowException("Authorization code is required")
