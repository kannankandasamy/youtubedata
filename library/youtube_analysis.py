import googleapiclient.discovery
import configparser
from library.utils import *

class Youtube:
    def __init__(self):
        conf = get_data_config()
        self.api_key = conf["api_key"]

    def get_api_connection(self):
        api_service_name = "youtube"
        api_version = "v3"
        youtube = googleapiclient.discovery.build(api_service_name, api_version, developerKey=self.api_key)
        return youtube     	

    
    #Get channel details
    def get_channel_details(self, youtube, channel_id):
        #channel_id = "UCpSLfHgOlcSBlkxRnMKcnVw"
        request = youtube.channels().list(
            part="snippet,contentDetails,statistics",
            id=channel_id
        )
        response = request.execute()
        for i in response['items']:
            data = dict(channel_name=i['snippet']['title'],
                        channel_id=i['id'],
                        channel_custom_url=i['snippet']['customUrl'],
                        video_count=i['statistics']['videoCount'],
                        view_count=i['statistics']['viewCount'],
                        subscriber_count=i['statistics']['subscriberCount'],
                        channel_description=i['snippet']['description'],
                        playlist_id=i['contentDetails']['relatedPlaylists']['uploads'],
                        )    
        return data    

    
    #Get Playlist details
    def get_playlist_details(self, youtube, channel_id):
        playlist_details = []
        next_page_token = None

        while True:
            request5=youtube.playlists().list(
                part="snippet,contentDetails",
                channelId=channel_id,
                maxResults=50,
                pageToken=next_page_token
            )
            response5 = request5.execute()
            #print(response5)

            for item in response5['items']:
                data = dict(playlist_id=item['id'],
                            playlist_title=item['snippet']['title'],
                            channel_id=item['snippet']['channelId'],
                            channel_name=item['snippet']['channelTitle'],
                            published_at=item['snippet']['publishedAt'],
                            video_count=item['contentDetails']['itemCount'])
                playlist_details.append(data)

            next_page_token=response5.get('nextPageToken')
            if next_page_token is None:
                break
        return playlist_details        


    #Get video id's and more header information
    def get_video_headers(self, youtube, channel_id):
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


    #Get video full details
    def get_video_full_details(self, youtube,vid_id_details):
        video_details = []
        for vid in vid_id_details:
            request3=youtube.videos().list(
                part="snippet,contentDetails,statistics",
                id=vid[0]
            )
            response3 = request3.execute()

            for item in response3["items"]:
                data = dict(video_title=item['snippet']['title'],
                            video_id=item['id'],
                            channel_name=item['snippet']['channelTitle'],
                            video_description=item['snippet'].get('description'),
                            video_tags=item['snippet'].get('tags'),
                            video_thumbnail=item['snippet']['thumbnails']['default']['url'],
                            video_view_count=item['statistics'].get('viewCount'),
                            video_like_count=item['statistics'].get('likeCount'),
                            video_comment_count=item['statistics'].get('commentCount'),
                            video_favorite_count=item['statistics'].get('favoriteCount'),
                            video_publishet_at=item['snippet']['publishedAt'],
                            video_duration=item['contentDetails']['duration'],
                            video_definition=item['contentDetails']['definition'],
                            video_caption=item['contentDetails']['caption'],
                            )
                video_details.append(data)
        return video_details


    #Get comments details
    def get_comments_full_details(self, youtube,vid_id_details):
        comment_details = []
        try:
            for vid in vid_id_details:
                request4=youtube.commentThreads().list(
                    part="snippet",
                    videoId=vid[0],
                    maxResults=50
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
        except:
            pass
        return comment_details