import logging

import pandas as pd
from psycopg2.extensions import connection
from psycopg2.extras import execute_batch

from airflow.decorators import task
from airflow.exceptions import AirflowException
from scripts.database.connection import connect_to_database
from scripts.utils.insert_query import (
    insert_dim_artist,
    insert_dim_date,
    insert_dim_song,
    insert_fact_song_stream,
)


def load_processed_data(filename: str) -> pd.DataFrame:
    try:
        csv_path = f"/opt/data/processed/{filename}"
        logging.info("Reading data to load.........")
        df = pd.read_csv(csv_path)
        return df

    except FileNotFoundError:
        logging.error("Processed data file not found.")
        raise AirflowException("Processed data file not found.")


def load_to_star_schema(df: pd.DataFrame, conn: connection):
    try:
        cursor = conn.cursor()
        logging.info("Inserting data to star schema.........")

        # upsert artists
        execute_batch(cursor, """
            INSERT INTO dim_artist (artist_name, artist_natural_key)
            VALUES (%s, %s)
            ON CONFLICT (artist_natural_key) DO NOTHING
        """, [(row["artist_name"], row["artist_natural_key"]) for _, row in df.iterrows()])

        # upsert songs
        execute_batch(cursor, """
            INSERT INTO dim_song (song_title, duration_ms, song_natural_key)
            VALUES (%s, %s, %s)
            ON CONFLICT (song_natural_key) DO NOTHING
        """, [(row["song_title"], int(row["song_duration_ms"]), row["song_natural_key"]) for _, row in df.iterrows()])

        # upsert dates
        execute_batch(cursor, """
            INSERT INTO dim_date (year, month, hour_of_day, day_of_week)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (year, month, hour_of_day, day_of_week) DO NOTHING
        """, [(int(row["year"]), int(row["month"]), int(row["hour_of_day"]), row["day_of_week"].lower()) for _, row in df.iterrows()])

        # load fact table row by row (needs upsert logic)
        for _, row in df.iterrows():
            cursor.execute("SELECT artist_id FROM dim_artist WHERE artist_natural_key = %s", (row["artist_natural_key"],))
            artist_id = cursor.fetchone()[0]
            cursor.execute("SELECT song_id FROM dim_song WHERE song_natural_key = %s", (row["song_natural_key"],))
            song_id = cursor.fetchone()[0]
            cursor.execute("""
                SELECT date_id FROM dim_date
                WHERE year = %s AND month = %s AND hour_of_day = %s AND day_of_week = %s
            """, (int(row["year"]), int(row["month"]), int(row["hour_of_day"]), row["day_of_week"].lower()))
            date_id = cursor.fetchone()[0]
            insert_fact_song_stream(song_id, artist_id, date_id, cursor, row)

        logging.info("Data inserted successfully.........")
    except Exception as e:
        logging.error(f"Star schema load error: {e}")
        raise AirflowException(f"Star schema load error: {e}")


@task
def load():
    try:
        df = load_processed_data("processed_played_tracks.csv")

        logging.info("Connecting to database.........")
        conn = connect_to_database()

        load_to_star_schema(df, conn)

        conn.commit()
        conn.close()

    except Exception as e:
        logging.error("Loading error: ", e)
        raise AirflowException("Loading error: ", e)
