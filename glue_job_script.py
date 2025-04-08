
import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import explode, col
from pyspark.sql.functions import to_date
from pyspark.sql.functions import to_timestamp
from datetime import datetime
from awsglue.dynamicframe import DynamicFrame
import boto3

sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
s3_path = "s3://spotify-etl-project-ap/raw_data/to_processed/"
output_path = "s3://spotify-etl-project-ap/transformed_data/"
source_dyf = glueContext.create_dynamic_frame_from_options(
    connection_type="s3",
    connection_options={"paths":[s3_path]},
    format="json"
)
spotify_df = source_dyf.toDF()
df = spotify_df
from pyspark.sql.functions import col, to_date, explode, to_timestamp

def process_album(df):
    df_album = df.select(explode(col('items')).alias('items')).select(
        col('items.track.album.id').alias('album_id'),
        col('items.track.album.name').alias('album_name'),
        col('items.track.album.release_date').alias('album_release_date'),
        col('items.track.album.total_tracks').alias('album_total_tracks'),
        col('items.track.album.external_urls.spotify').alias('url')
    ).drop_duplicates(['album_id'])
    return df_album

def process_artist(df):
    df_items_exploded = df.select(explode(col('items')).alias('items'))
    
    df_artist_exploded = df_items_exploded.select(explode(col('items.track.artists')).alias('artists'))
  
    df_artist = df_artist_exploded.select(
        col('artists.id').alias('artist_id'),
        col('artists.name').alias('artist_name'),
        col('artists.external_urls.spotify').alias('url')
    ).drop_duplicates(['artist_id'])
    
    return df_artist

def process_song(df):
    
    df_songs = df.select(explode(col('items')).alias('items')).select(
        col('items.track.id').alias('song_id'),
        col('items.track.name').alias('song_name'),
        col('items.track.duration_ms').alias('duration_ms'),
        col('items.track.popularity').alias('popularity'),
        col('items.track.external_urls.spotify').alias('url'),
        col('items.track.album.id').alias('album_id'),
        col('items.track.artists')[0]["id"].alias('artist_id'),
        col('items.added_at').alias('song_added')
    ).drop_duplicates(['song_id'])
    
    df_songs = df_songs.withColumn("song_added", to_date(col("song_added")))
    
    return df_songs

df_album = process_album(df)
df_artist = process_artist(df)
df_songs = process_song(df)
                            
df_artist.show(5)
df_album.show(5)
df_songs.show(5)
def write_to_s3(df, path_suffix, format_type="csv"):
    dynamic_frame = DynamicFrame.fromDF(df, glueContext, "dynamic_frame")
    glueContext.write_dynamic_frame.from_options(
        frame=dynamic_frame,
        connection_type="s3",
        connection_options={"path": output_path + path_suffix},
        format=format_type
    )
    

write_to_s3(df_album, "album_data/album_transformed_{}".format(datetime.now().strftime("%Y%m%d")), "csv")
write_to_s3(df_artist, "artist_data/artist_transformed_{}".format(datetime.now().strftime("%Y%m%d")), "csv")
write_to_s3(df_songs, "song_data/song_transformed_{}".format(datetime.now().strftime("%Y%m%d")), "csv")

def list_s3_objects(bucket, prefix):
    s3_client = boto3.client('s3')
    response = s3_client.list_objects_v2(Bucket=bucket, Prefix=prefix)
    keys = [content['Key'] for content in response.get('Contents', []) if content['Key'].endswith('json')]
    return keys

def move_and_delete(spotify_keys, bucket):
    s3_resource = boto3.resource('s3')
    for key in spotify_keys:
        copy_source = {'Bucket': bucket, 'Key': key}
        destination_key = 'raw_data/processed/' + key.split("/")[-1]
        s3_resource.Object(bucket, destination_key).copy(copy_source)
        s3_resource.Object(bucket, key).delete()

bucket_name = "spotify-etl-project-ap"
prefix = "raw_data/to_processed/"
spotify_keys = list_s3_objects(bucket_name, prefix)

# Actually move and delete the files
move_and_delete(spotify_keys, bucket_name)

job.commit()
