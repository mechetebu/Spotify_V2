from datetime import datetime
from airflow.sdk import dag, task
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.hooks.base import BaseHook
from cosmos import (
    DbtTaskGroup,
    ProjectConfig,
    ProfileConfig,
    ExecutionConfig,
    RenderConfig,
)
from dotenv import load_dotenv
from pendulum import duration
import os

# import boto3
import duckdb

### TODO Parameterize bucket names, environments prepare for lift and shift to s3 once I get cosmos going


@dag(
    schedule="@daily",
    start_date=datetime(2025, 7, 20),
    tags=["Spotify_V2"],
    default_args={
        "retries": 3,
        "retry_delay": duration(seconds=10),
        "max_retry_delay": duration(hours=1),
    },
)
def ingestion_dag():

    @task
    def list_files():
        hook = S3Hook(aws_conn_id="S3")
        return hook.list_keys(bucket_name="raw-spotify-data", prefix="")

    @task
    def prepare_duck_db():
        conn = BaseHook.get_connection("S3")
        con = duckdb.connect(".\Spotify_V2\dev.duckdb")
        con.execute("INSTALL httpfs; LOAD httpfs;")
        con.execute("INSTALL json; LOAD json;")

        # Set MinIO credentials
        con.execute("SET s3_region='us-east-1';")
        con.execute(f"SET s3_access_key_id='{conn.login}';")
        con.execute(f"SET s3_url_style='path';")
        con.execute(f"SET s3_secret_access_key='{conn.password}';")
        con.execute(
            f'SET s3_endpoint="{conn.extra_dejson.get("endpoint_url").replace("http://localhost:9000", "localhost:9000")}";'
        )
        con.execute("SET s3_use_ssl=false;")
        table_name = "streaming_history"
        con.execute(f"DROP TABLE IF EXISTS {table_name};")
        con.execute(
            f"""
                    
        CREATE TABLE {table_name} (
            ts TIMESTAMP,
            platform TEXT,
            ms_played INTEGER,
            conn_country TEXT,
            ip_addr TEXT,
            master_metadata_track_name TEXT,
            master_metadata_album_artist_name TEXT,
            master_metadata_album_album_name TEXT,
            spotify_track_uri TEXT,
            episode_name TEXT,
            episode_show_name TEXT,
            spotify_episode_uri TEXT,
            audiobook_title TEXT,
            audiobook_uri TEXT,
            audiobook_chapter_uri TEXT,
            audiobook_chapter_title TEXT,
            reason_start TEXT,
            reason_end TEXT,
            shuffle BOOLEAN,
            skipped BOOLEAN,
            offline BOOLEAN,
            offline_timestamp BIGINT,  
            incognito_mode BOOLEAN
        );
                    """
        )
        return table_name

    @task
    def load_files_to_duck_db(json_files, table_name):
        conn = BaseHook.get_connection("S3")
        con = duckdb.connect(".\Spotify_V2\dev.duckdb")
        con.execute("INSTALL httpfs; LOAD httpfs;")
        con.execute("INSTALL json; LOAD json;")

        # Set MinIO credentials
        con.execute("SET s3_region='us-east-1';")
        con.execute(f"SET s3_access_key_id='{conn.login}';")
        con.execute(f"SET s3_url_style='path';")
        con.execute(f"SET s3_secret_access_key='{conn.password}';")
        con.execute(
            f'SET s3_endpoint="{conn.extra_dejson.get("endpoint_url").replace("http://minio:9000", "minio:9000")}";'
        )
        con.execute("SET s3_use_ssl=false;")
        table_name = "streaming_history"

        for file in json_files:
            print(file)
            con.execute(
                f"INSERT INTO {table_name} SELECT * FROM read_json_auto('s3://raw-spotify-data/{file}');"
            )

        print(f"Loaded {len(json_files)} JSON files into {table_name}")

    dbt_project = "/opt/airflow/dbt/Spotify_V2/"
    dbt_group = DbtTaskGroup(
        group_id="dbt_models",
        project_config=ProjectConfig(dbt_project_path="/opt/airflow/dbt/Spotify_V2/"),
        profile_config=ProfileConfig(
            profile_name="Spotify_V2",
            profiles_yml_filepath=dbt_project + "profiles.yml",
            target_name="dev",
        ),
        execution_config=ExecutionConfig(
            dbt_executable_path="/opt/airflow/dbt_venv/bin/dbt"
        ),
    )

    json_files = list_files()
    table_name = prepare_duck_db()
    load_files_to_duck_db(json_files, table_name) >> dbt_group


ingestion_dag()
