import pandas as pd
import streamlit as st

from library.youtube_analysis import *
from library.mongod_load import *
from library.mysql_load import *

class DataAnalytics:

    if __name__=="__main__":
        op = "SUCCESS"
        yd = Youtube()
        print(yd.api_key)

        youtube = yd.get_api_connection()    
        print(youtube)   
        print("Youtube object created")

        mongo = Mongod()

        #Channel to harvest
        channel_id = "UCkRFwipiIqBTakN-mkZ-GcQ"
        #op = mongo.load_youtube_details_mongo(yd, channel_id)
        #print(op)

        mys = Mysql()
        op = mys.load_channel_to_mysql_alchemy(mongo)
        #print(op)

        op = mys.load_playlist_to_mysql_alchemy(mongo)
        #print(op)

        op = mys.load_videos_to_mysql_alchemy(mongo)
        #print(op)        

        op = mys.load_comments_to_mysql_alchemy(mongo)
        print(op)