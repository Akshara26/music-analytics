import os
import requests
import base64
import logging
from airflow.exceptions import AirflowException
from airflow.decorators import task

logger = logging.getLogger(__name__)

@task
def authorize_user():
    logger.info("Authorizing user via OAuth refresh token...")

    client_id = os.environ.get("SPOTIFY_CLIENT_ID")
    client_secret = os.environ.get("SPOTIFY_CLIENT_SECRET")
    refresh_token = os.environ.get("SPOTIFY_REFRESH_TOKEN")

    if not all([client_id, client_secret, refresh_token]):
        raise AirflowException("Missing SPOTIFY_CLIENT_ID, SPOTIFY_CLIENT_SECRET, or SPOTIFY_REFRESH_TOKEN")

    credentials = base64.b64encode(f"{client_id}:{client_secret}".encode()).decode()

    response = requests.post(
        "https://accounts.spotify.com/api/token",
        headers={
            "Authorization": f"Basic {credentials}",
            "Content-Type": "application/x-www-form-urlencoded"
        },
        data={
            "grant_type": "refresh_token",
            "refresh_token": refresh_token
        }
    )

    if response.status_code != 200:
        raise AirflowException(f"Failed to get access token: {response.text}")

    access_token = response.json()["access_token"]

    # Save token to file where the rest of the pipeline expects it
    token_path = "/opt/data/token.txt"
    os.makedirs(os.path.dirname(token_path), exist_ok=True)
    with open(token_path, "w") as f:
        f.write(access_token)

    logger.info("Access token successfully obtained and saved.")
    return access_token
