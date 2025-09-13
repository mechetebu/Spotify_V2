import duckdb
import boto3
from urllib.parse import urljoin
import os
from dotenv import load_dotenv

load_dotenv()
AWS_S3_ENDPOINT = os.environ.get("AWS_S3_ENDPOINT")
AWS_ACCESS_KEY_ID = os.environ.get("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.environ.get("AWS_SECRET_ACCESS_KEY")
AWS_S3_ALLOW_UNSAFE_SSL = os.environ.get("AWS_S3_ALLOW_UNSAFE_SSL")


# ---------- Config ----------
minio_endpoint = AWS_S3_ENDPOINT
bucket_name = "raw-spotify-data"
#prefix = "events/2025-06-15/"   access_key = AWS_ACCESS_KEY_ID
secret_key = AWS_SECRET_ACCESS_KEY
access_key = AWS_ACCESS_KEY_ID

# ---------- Connect to MinIO ----------
s3 = boto3.client(
    's3',
    endpoint_url=minio_endpoint,
    aws_access_key_id=access_key,
    aws_secret_access_key=secret_key,
    use_ssl = False
)

# ---------- List JSON Files ----------
response = s3.list_objects_v2(Bucket=bucket_name) #, Prefix=prefix)
json_files = [
    f"s3://{bucket_name}/{obj['Key']}"
    for obj in response.get('Contents', [])
    if obj['Key'].endswith('.json')
]
#print(f'Responses: {response}')
print(f'JSON files: {json_files[0]}')
# ---------- Connect to DuckDB ----------
print(os.getcwd())
con = duckdb.connect(".\Spotify_V2\dev.duckdb")
con.execute("INSTALL httpfs; LOAD httpfs;")
con.execute("INSTALL json; LOAD json;")

# Set MinIO credentials
con.execute("SET s3_region='us-east-1';")
con.execute(f"SET s3_access_key_id='{access_key}';")
con.execute(f"SET s3_url_style='path';")
con.execute(f"SET s3_secret_access_key='{secret_key}';")
con.execute(f'SET s3_endpoint="{minio_endpoint.replace("http://localhost:9000", "localhost:9000")}";')
con.execute("SET s3_use_ssl=false;")

# ---------- Load JSON Files One by One ----------
table_name = "streaming_history"
con.execute(f"DROP TABLE IF EXISTS {table_name};")
con.execute("""
            
CREATE TABLE streaming_history (
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
            """)

#print(json_files[0])
#print(con.execute("SELECT * FROM read_json_auto('local_copy.json');").df())
#df = con.execute("SELECT * FROM read_json_objects('http://localhost:9000/raw-spotify-data/Streaming_History_Audio_2018_0.json', format = 'array');").df()
#df = con.execute(f"SELECT * FROM  read_json_auto('{json_files[0]}') LIMIT 5;").df()
#print(df)
#con.execute(f"CREATE TABLE {table_name} AS SELECT * FROM read_json_auto('{json_files[1]}');")
#query = con.execute(f"SELECT * FROM {table_name} LIMIT 1;").fetchall()
#print(query)
for file in json_files:
    print(file)
    con.execute(f"INSERT INTO {table_name} SELECT * FROM read_json_auto('{file}');")

print(f"Loaded {len(json_files)} JSON files into {table_name}")
