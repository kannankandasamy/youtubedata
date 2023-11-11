import pandas as pd
import streamlit as st
import googleapiclient.discovery
import configparser

def get_data_config():
    conf = {}
    cfg = configparser.ConfigParser()
    cfg.read('data.conf')
    for (key,val) in cfg.items('DATA_APP_CONFIGS'):
        conf[key]=val
    return conf

def get_api_connection(api_key):
    api_service_name = "youtube"
    api_version = "v3"
    youtube = googleapiclient.discovery.build(api_service_name, api_version, developerKey=api_key)
    return youtube



#Adding comments to include nextPageToken

def get_comments_full_details(youtube,vid_id_details):
    comment_details = []

    for vid in vid_id_details:
        next_page_token = None
        print(vid[0])
        print(len(comment_details))
        while True:
            try:
                request4=youtube.commentThreads().list(
                    part="snippet",
                    videoId=vid[0],
                    maxResults=100,
                    order="time",
                    textFormat="plainText",
                    pageToken=next_page_token
                )
                response4 = request4.execute()

                for item in response4["items"]:
                    data = dict(comment_id=item['snippet']['topLevelComment']['id'],
                                video_id=item['snippet']['topLevelComment']['snippet']['videoId'],
                                comment_text=item['snippet']['topLevelComment']['snippet']['textDisplay'],
                                comment_author=item['snippet']['topLevelComment']['snippet']['authorDisplayName'],
                                comment_published_at=item['snippet']['topLevelComment']['snippet']['publishedAt']
                                )
                    comment_details.append(data)
                if next_page_token is None:
                    break
            except:
                pass
    return comment_details

def get_video_headers(youtube, channel_id):
    #channel_id = "UCDjEYBdbvu-1DpWukvNu8vA"
    vid_ids = []
    request = youtube.channels().list(
        part="contentDetails",
        id=channel_id
    )
    response = request.execute()
    playlist_id=response['items'][0]['contentDetails']['relatedPlaylists']['uploads']

    next_page_token = None

    while True:
        request1 = youtube.playlistItems().list(
            part='snippet',
            playlistId=playlist_id,
            maxResults=50,
            pageToken=next_page_token
        )
        response1 = request1.execute()
        for i in range(len(response1['items'])):
            vid_ids.append((response1['items'][i]['snippet']['resourceId']['videoId'], response1['items'][i]['snippet']['publishedAt']))
        next_page_token=response1.get('nextPageToken')
        if next_page_token is None:
            break
    return vid_ids

api_key = get_data_config()
youtube = get_api_connection(api_key)

comment_details = []
request4=youtube.commentThreads().list(
    part="snippet",
    videoId="VO1TybI4pGc",
    maxResults=25,
    order="time",
    textFormat="plainText"
)
response4 = request4.execute()

for item in response4["items"]:
    data = dict(comment_id=item['snippet']['topLevelComment']['id'],
                video_id=item['snippet']['topLevelComment']['snippet']['videoId'],
                comment_text=item['snippet']['topLevelComment']['snippet']['textDisplay'],
                comment_author=item['snippet']['topLevelComment']['snippet']['authorDisplayName'],
                comment_published_at=item['snippet']['topLevelComment']['snippet']['publishedAt']
                )
    comment_details.append(data)

vid_id_details = get_video_headers(youtube, "UCDA9RcW2ftEWIh5wS6dkhJA")