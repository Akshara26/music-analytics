import logging
import pandas as pd
from airflow.decorators import task
from airflow.exceptions import AirflowException
from scripts.utils.save_df_csv import save_df_as_csv
from scripts.utils.tokens.generate_spotify_client import generate_spotify_client


def extract_single_track_data(track_item):
    """Extract relevant data for a single top track."""
    artists = track_item.get("artists", [{}])
    return {
        "song_title": track_item.get("name", "N/A"),
        "artist_name": artists[0].get("name", "N/A") if artists else "N/A",
        "played_at": pd.Timestamp.now().isoformat(),
        "song_duration_ms": track_item.get("duration_ms", 0),
        "artist_natural_key": artists[0].get("id") if artists else None,
        "song_natural_key": track_item.get("id"),
        "popularity": track_item.get("popularity", 0),
        "time_range": "medium_term",
    }


@task
def extract_spotify_recently_played() -> pd.DataFrame:
    """
    Fetch user's top tracks from Spotify (free tier compatible).
    Extracts top 50 tracks over medium_term (last ~6 months).
    """
    try:
        spotify_client = generate_spotify_client()

        logging.info("Extracting Spotify top tracks data........")
        top_tracks = spotify_client.current_user_top_tracks(
            limit=50, time_range="medium_term"
        )

        track_data = [
            extract_single_track_data(item)
            for item in top_tracks["items"]
        ]

        df_columns = [
            "song_title",
            "artist_name",
            "played_at",
            "song_duration_ms",
            "artist_natural_key",
            "song_natural_key",
            "popularity",
            "time_range",
        ]
        df = pd.DataFrame(track_data, columns=df_columns)

        logging.info(f"Extracted {len(df)} top tracks successfully.")
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