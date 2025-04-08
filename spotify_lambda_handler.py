
import json
import os
import spotipy
from spotipy.oauth2 import SpotifyClientCredentials
import boto3
from datetime import datetime

def lambda_handler(event, context):
    client_id = os.environ.get('client_id')
    client_secret = os.environ.get('client_secret')
    
    client_credentials_manager = SpotifyClientCredentials(client_id=client_id, client_secret=client_secret)
    sp = spotipy.Spotify(client_credentials_manager=client_credentials_manager)
    
    playlist_link = "https://open.spotify.com/playlist/2gTOS5ytCNAtI5qQVQsQ2m"
    playlist_URI = playlist_link.split("/")[-1].split("?")[0]
    
    spotify_data = sp.playlist_tracks(playlist_URI)
    
    client = boto3.client('s3')

    timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    filename = f"spotify_raw_{timestamp}.json"

    client.put_object(
        Bucket='spotify-etl-project-ap',
        Key='raw_data/to_processed/' + filename,
        Body=(json.dumps(spotify_data) + "\n").encode('utf-8')
    )

    glue = boto3.client('glue')
    glue_job_name = 'spark_transformation_job_test'

    try:
        runID = glue.start_job_run(JobName=glue_job_name)
        status = glue.get_job_run(JobName=glue_job_name, RunId=runID['JobRunId'])
        print("Job Status: ", status['JobRun']['JobRunState'])
        return status['JobRun']['JobRunState']
    except Exception as e:
        print("Error:", e)
        return "Error: " + str(e)
