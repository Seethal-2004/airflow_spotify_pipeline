CREATE WAREHOUSE glue_ware;
USE WAREHOUSE glue_ware;

CREATE DATABASE glue_base;
USE DATABASE glue_base;

CREATE SCHEMA glue_sch;
USE SCHEMA glue_sch;


CREATE STORAGE INTEGRATION s3_spotifyglue
TYPE = EXTERNAL_STAGE
STORAGE_PROVIDER = S3
ENABLED = TRUE
STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::111122223333:role/fake-placeholder-role'
STORAGE_ALLOWED_LOCATIONS = ('s3://ec2bucketglue/processed_data/spotify_clean_data.csv/');

DESC INTEGRATION s3_spotifyglue;

CREATE OR REPLACE STAGE glue_stage
  URL = 's3://ec2bucketglue/processed_data/spotify_clean_data.csv/'
  STORAGE_INTEGRATION = s3_spotifyglue
  FILE_FORMAT = (TYPE = CSV SKIP_HEADER=1 FIELD_OPTIONALLY_ENCLOSED_BY='"');

CREATE OR REPLACE TABLE spotify_data (
  Source STRING,
  Index_col STRING,
  ID STRING,
  Name STRING,
  URL STRING
);

CREATE OR REPLACE FILE FORMAT spotify_csv_format
TYPE = 'CSV'
FIELD_OPTIONALLY_ENCLOSED_BY = '"'
SKIP_HEADER = 1
FIELD_DELIMITER = ',';

COPY INTO spotify_data
FROM @glue_stage
-- FILES = ('part-00000-c0b76e3a-fedf-4a86-99e7-aff4c5b73371-c000.csv')
ON_ERROR = 'CONTINUE';

ALTER STORAGE INTEGRATION s3_spotifyglue
SET STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::378679034419:role/spotifysnowflake';

SELECT * FROM spotify_data;
