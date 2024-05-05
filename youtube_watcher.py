import requests
import json
import logging
import sys
from config import config
from confluent_kafka import SerializingProducer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.serialization import StringSerializer
from confluent_kafka.schema_registry.avro import AvroSerializer

def fetch_data(API_key, id, page_token):
    response = requests.get(
        'https://www.googleapis.com/youtube/v3/playlistItems', 
        params={
            'key': API_key, 
            'playlistId': id, 
            'part': 'contentDetails', 
            'pageToken': page_token
        }
    )
    payload = json.loads(response.text)
    return payload

def fetch_playlist_data(API_key, id, page_token=None):
    payload = fetch_data(API_key, id, page_token)
    next_page_token = payload.get('nextPageToken')
    playlist_items = payload['items']
    if next_page_token is not None:
        playlist_items += fetch_playlist_data(API_key, id, next_page_token)
    return playlist_items

def fetch_videos(API_key, id, page_token):
    response = requests.get(
        'https://www.googleapis.com/youtube/v3/videos', 
        params={
            'key': API_key, 
            'id': id, 
            'part': 'snippet,statistics', 
            'pageToken': page_token
        }
    )
    payload = json.loads(response.text)
    return payload

def fetch_video_data(API_key, id, page_token):
    payload = fetch_videos(API_key, id, page_token)
    next_page_token = payload.get('nextPageToken')
    videos = payload['items']
    if next_page_token is not None:
        videos += fetch_video_data(API_key, id, next_page_token)
    return videos

def summarize_video(video_item):
    return {
        'video_id': video_item['id'], 
        'video_timestamp': video_item['snippet']['publishedAt'], 
        'title': video_item['snippet']['title'], 
        'description': video_item['snippet']['description'], 
        'views': int(video_item['statistics']['viewCount']), 
        'likes': int(video_item['statistics']['likeCount']), 
        'favorites': int(video_item['statistics']['favoriteCount']), 
        'comments': int(video_item['statistics']['commentCount'])
    }

def on_delivery(err, record):
    pass

def main():
    API_key = config['google_api_key']
    playlist_id = config['yt_playlist_id']

    schema_registry_client = SchemaRegistryClient(config['schema_registry'])
    yt_videos_value_schema = schema_registry_client.get_latest_version('yt_videos-value')
    kafka_config = config['kafka'] | {
        'key.serializer': StringSerializer(), 
        'value.serializer': AvroSerializer(
            schema_registry_client, 
            yt_videos_value_schema.schema.schema_str
        )
    }
    producer = SerializingProducer(kafka_config)

    for playlist_item in fetch_playlist_data(API_key, playlist_id):
        video_id = playlist_item['contentDetails']['videoId']
        for video_item in fetch_video_data(API_key, video_id, None):
            # print(video_item)
            # print(json.dumps(video_item, indent=4))
            print(summarize_video(video_item))

            producer.produce(
                topic = 'yt_videos', 
                key = video_id, 
                value = {
                    'VIDEO_TIMESTAMP': video_item['snippet']['publishedAt'], 
                    'TITLE': video_item['snippet']['title'], 
                    'DESCRIPTION': video_item['snippet']['description'], 
                    'VIEWS': int(video_item['statistics']['viewCount']), 
                    'LIKES': int(video_item['statistics']['likeCount']), 
                    'FAVORITES': int(video_item['statistics']['favoriteCount']), 
                    'COMMENTS': int(video_item['statistics']['commentCount'])
                }, 
                on_delivery = on_delivery
            )
    producer.flush()

if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG)
    sys.exit(main())