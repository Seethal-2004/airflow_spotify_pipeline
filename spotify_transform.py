import json
import boto3
from datetime import datetime
from io import StringIO
import pandas as pd
from kafka import KafkaProducer

# ---------- Extract Functions ----------
def album(data):
    album_list = []
    for row in data['items']:
        album_id = row['track']['album']['id']
        album_name = row['track']['album']['name']
        album_release_date = row['track']['album']['release_date']
        album_total_tracks = row['track']['album']['total_tracks']
        album_url = row['track']['album']['external_urls']['spotify']
        album_element = {
            'album_id': album_id, 'name': album_name, 'release_date': album_release_date,
            'total_tracks': album_total_tracks, 'url': album_url
        }
        album_list.append(album_element)
    return album_list

def art(artist_data):
    artist_list = []
    for row in artist_data['items']:
        for artist in row['track']['artists']:
            artist_dict = {
                'artist_id': artist['id'],
                'artist_name': artist['name'],
                'external_url': artist['href']
            }
            artist_list.append(artist_dict)
    return artist_list

def song(song_data):
    song_list = []
    for row in song_data['items']:
        song_id = row['track']['id']
        song_name = row['track']['name']
        song_duration = row['track']['duration_ms']
        song_url = row['track']['external_urls']['spotify']
        song_popularity = row['track']['popularity']
        song_added = row['added_at']
        song_element = {
            'song_id': song_id, 'song_name': song_name, 'duration_ms': song_duration, 'url': song_url,
            'popularity': song_popularity, 'song_added': song_added
        }
        song_list.append(song_element)
    return song_list

# ---------- Kafka Send Function ----------
def send_to_kafka(data_list, topic_name, bootstrap_servers):
    producer = KafkaProducer(
        bootstrap_servers=bootstrap_servers,
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )
    for record in data_list:
        producer.send(topic_name, record)
    producer.flush()
    producer.close()

# ---------- Lambda Handler ----------
def lambda_handler(event, context):
    """
    Lambda function that:
    1. Reads Spotify JSON files from S3.
    2. Extracts album, artist, and song data.
    3. Uploads transformed CSVs to S3.
    4. Sends data to Kafka.
    5. Moves processed files to 'processed_data' folder.

    Expects in the event:
    {
        "bucket_name": "<bucket-name>",
        "raw_data_prefix": "<prefix-to-read-files>",
        "album_prefix": "<album-output-prefix>",
        "artist_prefix": "<artist-output-prefix>",
        "song_prefix": "<song-output-prefix>"
    }
    """

    # --- Read dynamic parameters or fallback to defaults ---
    bucket_name = event.get('bucket_name', 'ec2bucketkafka')
    raw_data_prefix = event.get('raw_data_prefix', 'raw_data/processed_data/')
    album_prefix = event.get('album_prefix', 'transformed_data/album_data/')
    artist_prefix = event.get('artist_prefix', 'transformed_data/artist_data/')
    song_prefix = event.get('song_prefix', 'transformed_data/songs_data/')

    spotify_data = []
    spotify_key = []

    # Kafka broker config
    bootstrap_servers = ['13.127.44.58:9092']

    s3 = boto3.client("s3")

    # --- Read JSON files from S3 ---
    response = s3.list_objects(Bucket=bucket_name, Prefix=raw_data_prefix)
    if 'Contents' in response:
        for file in response['Contents']:
            file_key = file['Key']
            if file_key.endswith('.json'):
                data = s3.get_object(Bucket=bucket_name, Key=file_key)
                json_data = json.loads(data['Body'].read())
                spotify_data.append(json_data)
                spotify_key.append(file_key)
    else:
        print(f"No files found in bucket '{bucket_name}' with prefix '{raw_data_prefix}'.")
        return

    # --- Process and upload data ---
    for data in spotify_data:
        # ----- Album -----
        album_list = album(data)
        album_df = pd.DataFrame(album_list).drop_duplicates(subset=['album_id'])
        album_df['release_date'] = pd.to_datetime(album_df['release_date'], errors='coerce')
        album_key = album_prefix + 'album_transformed_' + datetime.now().strftime('%Y-%m-%d_%H-%M-%S') + '.csv'
        album_buffer = StringIO()
        album_df.to_csv(album_buffer, index=False)
        s3.put_object(Bucket=bucket_name, Key=album_key, Body=album_buffer.getvalue())
        send_to_kafka(album_list, 'spotify-topic', bootstrap_servers)

    for artist_data in spotify_data:
        # ----- Artist -----
        artist_list = art(artist_data)
        artist_df = pd.DataFrame(artist_list).drop_duplicates(subset=['artist_id'])
        artist_key = artist_prefix + 'artist_transformed_' + datetime.now().strftime('%Y-%m-%d_%H-%M-%S') + '.csv'
        artist_buffer = StringIO()
        artist_df.to_csv(artist_buffer, index=False)
        s3.put_object(Bucket=bucket_name, Key=artist_key, Body=artist_buffer.getvalue())
        send_to_kafka(artist_list, 'spotify-topic', bootstrap_servers)

    for song_data in spotify_data:
        # ----- Song -----
        song_list = song(song_data)
        song_df = pd.DataFrame(song_list)
        song_df['song_added'] = pd.to_datetime(song_df['song_added'], errors='coerce')
        song_key = song_prefix + 'songs_transformed_' + datetime.now().strftime('%Y-%m-%d_%H-%M-%S') + '.csv'
        song_buffer = StringIO()
        song_df.to_csv(song_buffer, index=False)
        s3.put_object(Bucket=bucket_name, Key=song_key, Body=song_buffer.getvalue())
        send_to_kafka(song_list, 'spotify-topic', bootstrap_servers)

    # --- Move processed files ---
    s3_resource = boto3.resource('s3')
    for key in spotify_key:
        copy_source = {'Bucket': bucket_name, 'Key': key}
        dest_key = 'raw_data/processed_data/' + key.split('/')[-1]
        s3_resource.meta.client.copy(copy_source, bucket_name, dest_key)
        s3.delete_object(Bucket=bucket_name, Key=key)
