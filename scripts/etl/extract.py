import datetime
import logging
import pandas as pd
from airflow.decorators import task
from airflow.exceptions import AirflowException
from scripts.utils.save_df_csv import save_df_as_csv
from scripts.utils.tokens.generate_spotify_client import generate_spotify_client


def extract_single_track_data(track_item):
    """Extract relevant data for a single recently played track."""
    track = track_item["track"]
    artists = track.get("artists", [{}])
    return {
        "song_title": track.get("name", "N/A"),
        "artist_name": artists[0].get("name", "N/A") if artists else "N/A",
        "played_at": track_item.get("played_at", "N/A"),
        "song_duration_ms": track.get("duration_ms", 0),
        "artist_natural_key": artists[0].get("id") if artists else None,
        "song_natural_key": track.get("id"),
    }


@task
def extract_spotify_recently_played() -> pd.DataFrame:
    """
    Fetch recently played songs from Spotify API.
    Extracts last 50 tracks played in the past 24 hours.
    """
    try:
        spotify_client = generate_spotify_client()

        today = datetime.datetime.now()
        yesterday = today - datetime.timedelta(days=1)
        yesterday_unix_ms = int(yesterday.timestamp()) * 1000

        logging.info("Extracting Spotify recently played data........")
        recently_played = spotify_client.current_user_recently_played(
            limit=50, after=yesterday_unix_ms
        )

        track_data = [
            extract_single_track_data(item)
            for item in recently_played["items"]
        ]

        df_columns = [
            "song_title",
            "artist_name",
            "played_at",
            "song_duration_ms",
            "artist_natural_key",
            "song_natural_key",
        ]
        df = pd.DataFrame(track_data, columns=df_columns)

        logging.info(f"Extracted {len(df)} recently played tracks successfully.")
        return df

    except Exception as e:
        logging.error(f"Error during extraction of data from Spotify: {e}")
        raise AirflowException(f"Error during extraction of data from Spotify: {e}")


@task
def save_to_staging_csv(df: pd.DataFrame):
    """Store extracted data to staging layer as csv file."""
    csv_filename = "staging_played_tracks.csv"
    csv_path = f"/opt/data/staging/{csv_filename}"

    logging.info("Saving extracted data to csv.....")
    save_df_as_csv(df, csv_path)
    logging.info("Extracted data saved in csv.......")