import spotipy
from spotipy.oauth2 import SpotifyClientCredentials
from kafka import KafkaProducer
from dotenv import load_dotenv
import os
load_dotenv()
producer= KafkaProducer(bootstrap_servers=['localhost:29092'],value_serializer=lambda x: x.encode('utf-8'))
print('Starting Spotify Producer')


client_id=os.getenv('SPOTIFY_CLIENTID')
client_secret=os.getenv('SPOTIFY_SECRET')

client_credentials_manager = SpotifyClientCredentials(client_id, client_secret)
sp = spotipy.Spotify(client_credentials_manager = client_credentials_manager)

def getTrackID(user,playlistid):
    id=[]
    playlist=sp.user_playlist(user,playlistid)
    for item in playlist['tracks']['item']:
        track=item['track']
        id.append(item['id'])
    return id