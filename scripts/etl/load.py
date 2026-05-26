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

        # bulk upsert artists
        execute_batch(cursor, """
            INSERT INTO dim_artist (artist_name, artist_natural_key)
            VALUES (%s, %s)
            ON CONFLICT (artist_natural_key) DO NOTHING
        """, [(row["artist_name"], row["artist_natural_key"]) for _, row in df.iterrows()])

        # bulk upsert songs
        execute_batch(cursor, """
            INSERT INTO dim_song (song_title, duration_ms, song_natural_key)
            VALUES (%s, %s, %s)
            ON CONFLICT (song_natural_key) DO NOTHING
        """, [(row["song_title"], int(row["song_duration_ms"]), row["song_natural_key"]) for _, row in df.iterrows()])

        # bulk upsert dates
        execute_batch(cursor, """
            INSERT INTO dim_date (year, month, hour_of_day, day_of_week)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (year, month, hour_of_day, day_of_week) DO NOTHING
        """, [(int(row["year"]), int(row["month"]), int(row["hour_of_day"]), row["day_of_week"].lower()) for _, row in df.iterrows()])

        # build lookup dicts from dim tables — single query each, no per-row round trips
        natural_keys = df["artist_natural_key"].tolist()
        cursor.execute(
            "SELECT artist_natural_key, artist_id FROM dim_artist WHERE artist_natural_key = ANY(%s)",
            (natural_keys,)
        )
        artist_lookup = {row[0]: row[1] for row in cursor.fetchall()}

        song_keys = df["song_natural_key"].tolist()
        cursor.execute(
            "SELECT song_natural_key, song_id FROM dim_song WHERE song_natural_key = ANY(%s)",
            (song_keys,)
        )
        song_lookup = {row[0]: row[1] for row in cursor.fetchall()}

        cursor.execute("""
            SELECT year, month, hour_of_day, day_of_week, date_id FROM dim_date
            WHERE (year, month, hour_of_day, day_of_week) IN %s
        """, (tuple(
            (int(row["year"]), int(row["month"]), int(row["hour_of_day"]), row["day_of_week"].lower())
            for _, row in df.iterrows()
        ),))
        date_lookup = {(r[0], r[1], r[2], r[3]): r[4] for r in cursor.fetchall()}

        # upsert fact table using lookups
        for _, row in df.iterrows():
            artist_id = artist_lookup[row["artist_natural_key"]]
            song_id = song_lookup[row["song_natural_key"]]
            date_key = (int(row["year"]), int(row["month"]), int(row["hour_of_day"]), row["day_of_week"].lower())
            date_id = date_lookup[date_key]

            cursor.execute("""
                SELECT song_stream_id, play_count, total_duration
                FROM fact_song_stream
                WHERE date_id = %s AND song_id = %s AND artist_id = %s
            """, (date_id, song_id, artist_id))
            existing = cursor.fetchone()

            if existing:
                cursor.execute("""
                    UPDATE fact_song_stream
                    SET play_count = %s, total_duration = %s
                    WHERE song_stream_id = %s
                """, (existing[1] + 1, existing[2] + int(row["song_duration_ms"]), existing[0]))
            else:
                cursor.execute("""
                    INSERT INTO fact_song_stream (date_id, song_id, artist_id, play_count, total_duration)
                    VALUES (%s, %s, %s, %s, %s)
                """, (date_id, song_id, artist_id, 1, int(row["song_duration_ms"])))

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
