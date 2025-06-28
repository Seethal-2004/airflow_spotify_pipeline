from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import timedelta
import boto3
import snowflake.connector
from dotenv import load_dotenv
import os
import json
from airflow.models import Variable

# Load .env file explicitly from the Docker volume
load_dotenv('/opt/airflow/.env')  # Adjust this if your .env is in a different mount

# Load AWS credentials and region from env
AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
AWS_REGION = os.getenv("AWS_DEFAULT_REGION", "ap-south-1")

SPOTIFY_LAMBDA_BUCKET = os.getenv('SPOTIFY_LAMBDA_BUCKET')
PROCESS_AND_SEND_BUCKET = os.getenv('PROCESS_AND_SEND_BUCKET')
GLUE_JOB_BUCKET = os.getenv('GLUE_JOB_BUCKET')

# Define the S3 prefixes
RAW_DATA_PREFIX = 'raw_data/processed_data/'
ALBUM_PREFIX = 'transformed_data/album_data/'
ARTIST_PREFIX = 'transformed_data/artist_data/'
SONG_PREFIX = 'transformed_data/songs_data/'
GLUE_PREFIX = 'processed_data/spotify_clean_data.csv/'

# Load Snowflake credentials from env
SNOWFLAKE_USER = os.getenv("SNOWFLAKE_USER")
SNOWFLAKE_PASSWORD = os.getenv("SNOWFLAKE_PASSWORD")
SNOWFLAKE_ACCOUNT = os.getenv("SNOWFLAKE_ACCOUNT")
SNOWFLAKE_WAREHOUSE = os.getenv("SNOWFLAKE_WAREHOUSE", "glue_ware")
SNOWFLAKE_DATABASE = os.getenv("SNOWFLAKE_DATABASE", "glue_base")
SNOWFLAKE_SCHEMA = os.getenv("SNOWFLAKE_SCHEMA", "glue_sch")

# Create AWS clients once, reuse inside functions
lambda_client = boto3.client(
    'lambda',
    region_name=AWS_REGION,
    aws_access_key_id=AWS_ACCESS_KEY_ID,
    aws_secret_access_key=AWS_SECRET_ACCESS_KEY
)

glue_client = boto3.client(
    'glue',
    region_name=AWS_REGION,
    aws_access_key_id=AWS_ACCESS_KEY_ID,
    aws_secret_access_key=AWS_SECRET_ACCESS_KEY
)

def fetch_from_spotify_lambda():
    try:
        payload = {
            "bucket_name": SPOTIFY_LAMBDA_BUCKET,
            "raw_data_prefix": RAW_DATA_PREFIX
        }
        response = lambda_client.invoke(
            FunctionName='sfunction',
            InvocationType='RequestResponse',
            Payload=json.dumps(payload).encode()
        )
        payload_str = response['Payload'].read().decode('utf-8')
        print(f"Spotify Lambda Response (bucket: {SPOTIFY_LAMBDA_BUCKET}):", payload_str)
    except Exception as e:
        print(f"Error invoking Spotify Lambda: {e}")
        raise

def process_and_send_lambda():
    try:
        payload = {
            "bucket_name": SPOTIFY_LAMBDA_BUCKET,
            "raw_data_prefix": RAW_DATA_PREFIX,
            "album_prefix": ALBUM_PREFIX,
            "artist_prefix": ARTIST_PREFIX,
            "song_prefix": SONG_PREFIX
        }
        response = lambda_client.invoke(
            FunctionName='ec2kafkaa',
            InvocationType='RequestResponse',
            Payload=json.dumps(payload).encode()
        )
        payload_str = response['Payload'].read().decode('utf-8')
        print(f"Process & Send Lambda Response (bucket: {SPOTIFY_LAMBDA_BUCKET}):", payload_str)
    except Exception as e:
        print(f"Error invoking Process & Send Lambda: {e}")
        raise

def run_glue_job():
    # GLUE_JOB_BUCKET = os.getenv('GLUE_JOB_BUCKET')
    # GLUE_JOB_BUCKET = Variable.get("GLUE_JOB_BUCKET", default_var=None)
    # print(f"GLUE_JOB_BUCKET is: {GLUE_JOB_BUCKET}")
    # if not GLUE_JOB_BUCKET:
    #     raise ValueError("GLUE_JOB_BUCKET environment variable is missing!")
    try:
        response = glue_client.start_job_run(
            JobName='spotify_job',
            Arguments={
                '--bucket_name': GLUE_JOB_BUCKET,
                '--glue_prefix': GLUE_PREFIX
            }
        )
        print(f"Glue Job started with bucket {GLUE_JOB_BUCKET}:", response['JobRunId'])
    except Exception as e:
        print(f"Error starting Glue job: {e}")
        raise


def load_to_snowflake():
    try:
        conn = snowflake.connector.connect(
            user=SNOWFLAKE_USER,
            password=SNOWFLAKE_PASSWORD,
            account=SNOWFLAKE_ACCOUNT,
            warehouse=SNOWFLAKE_WAREHOUSE,
            database=SNOWFLAKE_DATABASE,
            schema=SNOWFLAKE_SCHEMA
        )
        cs = conn.cursor()
        try:
            # Step 1: Truncate the table before loading
            cs.execute("TRUNCATE TABLE IF EXISTS spotify_data;")

            # Step 2: Copy new data into the table
            cs.execute("""
                COPY INTO spotify_data
                FROM @glue_stage
                FILE_FORMAT = (FORMAT_NAME = 'spotify_csv_format')
                ON_ERROR = 'CONTINUE';
            """)
            print("✅ Data loaded into Snowflake.")
        finally:
            cs.close()
            conn.close()
    except Exception as e:
        print(f"❌ Error loading data to Snowflake: {e}")
        raise


default_args = {
    'owner': 'spotify_etl',
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

from pendulum import datetime as pendulum_datetime

with DAG(
    dag_id='spotify_data_pipeline',
    default_args=default_args,
    start_date=pendulum_datetime(2025, 6, 6, tz="Asia/Kolkata"),
    schedule_interval='@hourly',
    catchup=False
) as dag:

    fetch_from_spotify = PythonOperator(
        task_id='fetch_from_spotify_lambda',
        python_callable=fetch_from_spotify_lambda
    )

    process_and_send = PythonOperator(
        task_id='process_and_send_lambda',
        python_callable=process_and_send_lambda
    )

    glue_task = PythonOperator(
        task_id='run_glue_job',
        python_callable=run_glue_job
    )

    load_snowflake = PythonOperator(
        task_id='load_to_snowflake',
        python_callable=load_to_snowflake
    )

    fetch_from_spotify >> process_and_send >> glue_task >> load_snowflake
