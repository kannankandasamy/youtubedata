import pymongo
import pandas as pd
from library.youtube_analysis import *

class Mongod:
    def __init__(self):
        self.client = pymongo.MongoClient("mongodb://localhost:27017/")

    def load_youtube_details_mongo(self, youtube_obj, channel_id):
        youtube = youtube_obj.get_api_connection()
        channel_details = youtube_obj.get_channel_details(youtube, channel_id)
        print("completed channel details")
        playlist_details = youtube_obj.get_playlist_details(youtube, channel_id)
        print("completed playlist details")
        vid_id_details = youtube_obj.get_video_headers(youtube, channel_id)
        print("completed getting video ids details")
        video_full_details = youtube_obj.get_video_full_details(youtube,vid_id_details)
        print("completed videos details")
        comment_details = youtube_obj.get_comments_full_details(youtube,vid_id_details)
        print("completed comments details")        
        
        db = self.client["youtubedb"]
        coll = db["youtube_channel_details"]
        coll.insert_one({"channel_details":channel_details,
                        "playlist_details":playlist_details,
                        "video_details":video_full_details,
                        "comments_details":comment_details})

        return "Upload Successfull"    


    def get_data_from_mongo(self, details):
        ch_list = []
        db = self.client["youtubedb"]
        coll1 = db["youtube_channel_details"]
        for i in coll1.find({},{"_id":0,details:1}):
            ch_list.append(i[details])
        df = pd.DataFrame(ch_list)
        return df        

    def get_data_from_mongo_with_array(self, details):
        ch_list = []
        db = self.client["youtubedb"]
        coll1 = db["youtube_channel_details"]
        for data in coll1.find({},{"_id":0,details:1}):
            for i in range(len(data[details])):
                ch_list.append(data[details][i])
        df = pd.DataFrame(ch_list)
        return df
