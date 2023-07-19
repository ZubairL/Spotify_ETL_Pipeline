import pandas as pd
from requests import post, get
import csv
import boto3
import datetime



def lambda_handler(event, context):
    #Authorization/Adminsistrative
    client_id = ("ea806ef90114449d8eddeb1d2fb2ed76")
    client_secret = ("c63f6ad3081549a7b301d04658f71c18")
    AUTH_URL = 'https://accounts.spotify.com/api/token'
    
    # POST
    auth_response =post(AUTH_URL, {
        'grant_type': 'client_credentials',
        'client_id': client_id,
        'client_secret': client_secret,
    })
    
    auth_response_data = auth_response.json()
    access_token = auth_response_data['access_token']
    headers = {'Authorization': 'Bearer {token}'.format(token=access_token)}
    BASE_URL = 'https://api.spotify.com/v1/'
    
    # TOP 50 URI ID
    track_id = '37i9dQZEVXbMDoHDwVN2tF'
    
    # GET request with proper header
    r = get(BASE_URL + 'playlists/' + track_id + '/tracks',
                      headers=headers)
    all_tracks_json = r.json()
    track_info=[]
    
    for i in all_tracks_json['items']:
        track_info.append((i["track"]["id"]))
    
    feature_dict_one = {}
    song_names = []
    artist_names = []
    
    for song in all_tracks_json["items"]:
        song_names.append(song["track"]["name"])
        artist_names.append(song["track"]["album"]["artists"][0]["name"])
        
    #print(track_info[1])
    feature_dict_one = {"song_name" : song_names,
                    "artist_name": artist_names}
    names_df = pd.DataFrame(feature_dict_one)
    
    
    
    audio_features_dict = {}
    audio_features_list = []
    
    
    response = get(BASE_URL + 'playlists/' + track_id + '/tracks', headers=headers)
    data = response.json()
    
    
    # Iterate over playlist tracks
    for item in data['items']:
        track = item['track']
        track_id = track['id']
        
        # GET request to retrieve audio features for the track
        response = get(BASE_URL + 'audio-features/' + track_id, headers=headers)
        track_features = response.json()
        
        # Append the audio features to the list as a dictionary
        audio_features_dict = {
            'Danceability': track_features['danceability'],
            'Energy': track_features['energy'],
            'Speechiness': track_features['speechiness'],
            'Instrumentalness': track_features['instrumentalness'],
            'Tempo': track_features['tempo']
        }
        audio_features_list.append(audio_features_dict)
    
        # Create DataFrame from the audio features list
        audio_df = pd.DataFrame(audio_features_list)
        
        
    total_df = pd.concat([names_df, audio_df], axis = 1)
    final_data_csv = total_df.to_csv(index=False)

    #Using datetime to generate todays date:
    current_date = datetime.datetime.now().strftime("%m-%d-%Y")




    #print(total_df.head(5))
    csv_data = final_data_csv

    # Upload CSV to Amazon S3
    bucket_name = 'zubairspotifybucket'
    file_key = f"input/spotify_data_csv_datefiles/data_{current_date}.csv"


    upload_csv_to_s3(csv_data, bucket_name, file_key)
    
    

    return {
        'statusCode': 200,
        'body': 'CSV file generated and uploaded successfully'
        }

def upload_csv_to_s3(csv_data, bucket_name, file_name):
    s3 = boto3.client('s3')
    
    try:
        response = s3.put_object(Body=csv_data, Bucket=bucket_name, Key=file_name)
        print(f"CSV file '{file_name}' uploaded successfully to S3 bucket '{bucket_name}'.")
    except Exception as e:
        print(f"Error uploading CSV file to S3 bucket: {str(e)}")
        
















