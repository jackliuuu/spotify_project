import spotipy
from spotipy.oauth2 import SpotifyClientCredentials
from kafka import KafkaProducer


producer= KafkaProducer(bootstrap_servers=['localhost:29092'],value_serializer=lambda x: x.encode('utf-8'))
print('Starting Spotify Producer')