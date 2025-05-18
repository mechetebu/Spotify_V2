import boto3
from botocore.client import Config
import os
from dotenv import load_dotenv
load_dotenv()
AWS_ACCESS_KEY_ID =  os.environ.get("MINIO_USER")
AWS_SECRET_ACCESS_KEY = os.environ.get("MINIO_PASSWORD")
ENDPOINT_URL = os.environ.get("ENDPOINT_UR")
s3_client = boto3.client("s3", endpoint_url = ENDPOINT_URL,aws_access_key_id = AWS_ACCESS_KEY_ID, aws_secret_access_key = AWS_SECRET_ACCESS_KEY, config = Config(signature_version='s3v4'), region_name = 'us-east-1')

print(s3_client.list_buckets())