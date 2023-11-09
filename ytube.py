import pandas as pd
import streamlit as st
import googleapiclient.discovery


class Youtube:
    def __init__(self, api_key):
        self.api_key = api_key

	def get_api_connection(self):
		api_service_name = "youtube"
		api_version = "v3"
		youtube = googleapiclient.discovery.build(api_service_name, api_version, developerKey=api_key)
		return youtube     											   

class Mongod:
    def __init__(self, connection):
        self.conn = connection

class MySql:
    def __init__(self, connection):
        self.conn = connection

class DataAnalytics:
    if __name__=="__main__":

        api_key = "AIzaSyAnCVRvszsKKvapt5MMuaBnHxpbV3KE-3M"
        yd = Youtube(api_key)
        print(yd.api_key)
		
        youtube = yd.get_api_connection()       
