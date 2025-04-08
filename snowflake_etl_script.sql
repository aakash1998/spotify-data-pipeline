
-- Create database
CREATE OR REPLACE DATABASE spotify_db;

-- Create file format
CREATE OR REPLACE FILE FORMAT spotify_csv_format
  TYPE = 'CSV'
  FIELD_OPTIONALLY_ENCLOSED_BY = '"'
  SKIP_HEADER = 1
  NULL_IF = ('\N', 'null', '');

-- Create stage
CREATE OR REPLACE STAGE spotify_stage
  URL = 's3://spotify-etl-project-ap/transformed_data/'
  STORAGE_INTEGRATION = spotify_s3_integration
  FILE_FORMAT = spotify_csv_format;

-- Create tables
CREATE OR REPLACE TABLE tbl_songs (
  song_id STRING,
  song_name STRING,
  duration_ms INT,
  popularity INT,
  url STRING,
  album_id STRING,
  artist_id STRING,
  song_added DATE
);

CREATE OR REPLACE TABLE tbl_artists (
  artist_id STRING,
  artist_name STRING,
  url STRING
);

CREATE OR REPLACE TABLE tbl_albums (
  album_id STRING,
  album_name STRING,
  album_release_date STRING,
  album_total_tracks INT,
  url STRING
);

-- Load initial data
COPY INTO tbl_songs
FROM @spotify_stage/song_data/song_transformed_20250406/run-1743974142544-part-r-00000
FILE_FORMAT = spotify_csv_format
ON_ERROR = CONTINUE;

COPY INTO tbl_artists
FROM @spotify_stage/artist_data/artist_transformed_20250406/run-1743974018138-part-r-00000
FILE_FORMAT = spotify_csv_format
ON_ERROR = CONTINUE;

COPY INTO tbl_albums
FROM @spotify_stage/album_data/album_transformed_20250406/run-1743973556172-part-r-00000
FILE_FORMAT = spotify_csv_format
ON_ERROR = CONTINUE;

-- Create pipes
CREATE OR REPLACE PIPE tbl_songs_pipe
AUTO_INGEST = TRUE
AS
COPY INTO tbl_songs
FROM @spotify_stage/song_data/
FILE_FORMAT = spotify_csv_format;

CREATE OR REPLACE PIPE tbl_artists_pipe
AUTO_INGEST = TRUE
AS
COPY INTO tbl_artists
FROM @spotify_stage/artist_data/
FILE_FORMAT = spotify_csv_format;

CREATE OR REPLACE PIPE tbl_albums_pipe
AUTO_INGEST = TRUE
AS
COPY INTO tbl_albums
FROM @spotify_stage/album_data/
FILE_FORMAT = spotify_csv_format;

-- Describe pipes
DESC PIPE tbl_songs_pipe;
DESC PIPE tbl_albums_pipe;
DESC PIPE tbl_artists_pipe;

-- Sample queries
SELECT * FROM tbl_artists ORDER BY artist_id DESC LIMIT 10;

SELECT * FROM TABLE(INFORMATION_SCHEMA.COPY_HISTORY(
  TABLE_NAME => 'tbl_albums',
  START_TIME => DATEADD(HOUR, -1, CURRENT_TIMESTAMP())));

LIST @spotify_stage/album_data/;

COPY INTO tbl_albums
FROM @spotify_stage/album_data/album_transformed_20250406/run-1743973556172-part-r-test3
FILE_FORMAT = (FORMAT_NAME = spotify_csv_format)
ON_ERROR = CONTINUE;

SELECT * FROM tbl_albums WHERE album_id = '3BoUxfC7YhxNq3TpOfnRif';

SELECT * FROM tbl_albums;
SELECT * FROM tbl_artists;
