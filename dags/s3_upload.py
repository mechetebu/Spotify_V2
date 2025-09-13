import boto3
from airflow.decorators import dag, task
from pendulum import datetime
from botocore.client import Config
import os
from dotenv import load_dotenv
from airflow.hooks.base import BaseHook
load_dotenv()
AWS_S3_ENDPOINT = os.environ.get("AWS_S3_ENDPOINT")
AWS_ACCESS_KEY_ID = os.environ.get("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.environ.get("AWS_SECRET_ACCESS_KEY")
AWS_S3_ALLOW_UNSAFE_SSL = os.environ.get("AWS_S3_ALLOW_UNSAFE_SSL")

@dag(      
start_date = datetime(2025,5,20),
schedule = "@daily",
tags= ['Spotify_V2'],
catchup = False
)
def load_duckdb():
    
    @task
    def testcreds():

    @task
    def load_data():
        print("The task works!")
    
    load_data()




dag_instance = load_duckdb()
