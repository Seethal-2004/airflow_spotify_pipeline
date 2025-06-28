import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from pyspark.context import SparkContext
from pyspark.sql.functions import from_json, col, lit, trim, row_number
from pyspark.sql.types import StructType, StringType, IntegerType, TimestampType
from pyspark.sql.window import Window

# Initialize Spark and Glue
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

# Parse dynamic parameters
args = getResolvedOptions(sys.argv, ['bucket_name', 'glue_prefix'])
bucket_name = args['bucket_name']
glue_prefix = args['glue_prefix']

# Kafka config
kafka_bootstrap_servers = "13.127.44.58:9092"
kafka_topic = "spotify-topic"

# Define JSON schema for parsing
schema = StructType() \
    .add("song_id", StringType()) \
    .add("song_name", StringType()) \
    .add("url", StringType()) \
    .add("popularity", IntegerType()) \
    .add("song_added", TimestampType()) \
    .add("album_id", StringType()) \
    .add("name", StringType()) \
    .add("artist_id", StringType()) \
    .add("artist_name", StringType()) \
    .add("external_url", StringType())

# Read data from Kafka (starting from earliest offset)
df_raw = spark.read \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
    .option("subscribe", kafka_topic) \
    .option("startingOffsets", "earliest") \
    .load()

# Parse JSON from Kafka messages
df_parsed = df_raw.selectExpr("CAST(value AS STRING) as value") \
    .withColumn("json_data", from_json(col("value"), schema))

# Albums
albums_df = df_parsed.select(
    lit("Album").alias("SOURCE"),
    trim(col("json_data.album_id")).alias("ID"),
    trim(col("json_data.name")).alias("NAME"),
    trim(col("json_data.url")).alias("URL")
).dropna(subset=["ID"]).dropDuplicates(["ID"])

# Artists
artists_df = df_parsed.select(
    lit("Artist").alias("SOURCE"),
    trim(col("json_data.artist_id")).alias("ID"),
    trim(col("json_data.artist_name")).alias("NAME"),
    trim(col("json_data.external_url")).alias("URL")
).dropna(subset=["ID"]).dropDuplicates(["ID"])

# Songs
songs_df = df_parsed.select(
    lit("Song").alias("SOURCE"),
    trim(col("json_data.song_id")).alias("ID"),
    trim(col("json_data.song_name")).alias("NAME"),
    trim(col("json_data.url")).alias("URL")
).dropna(subset=["ID"]).dropDuplicates(["ID"])

# Reassign row numbers per SOURCE
window_album = Window.orderBy("NAME")
albums_indexed = albums_df.withColumn("INDEX", row_number().over(window_album)) \
    .select("SOURCE", "INDEX", "ID", "NAME", "URL")

window_artist = Window.orderBy("NAME")
artists_indexed = artists_df.withColumn("INDEX", row_number().over(window_artist)) \
    .select("SOURCE", "INDEX", "ID", "NAME", "URL")

window_song = Window.orderBy("NAME")
songs_indexed = songs_df.withColumn("INDEX", row_number().over(window_song)) \
    .select("SOURCE", "INDEX", "ID", "NAME", "URL")

# Combine all three DataFrames in order: Album, Artist, Song
final_df = albums_indexed.unionByName(artists_indexed).unionByName(songs_indexed)

# Save to S3 with header and overwrite mode
output_path = f"s3://{bucket_name}/{glue_prefix}"
final_df.coalesce(1).write \
    .mode("overwrite") \
    .option("header", "true") \
    .csv(output_path)

print(f"✔ Deduplicated Spotify data saved to S3 at: {output_path}")
