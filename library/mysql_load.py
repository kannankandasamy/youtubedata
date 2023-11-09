import mysql.connector
import pandas as pd

class Mysql:
    def __init__(self):
        self.cnx = mysql.connector.Connect(user="root",password="admin", host="127.0.0.1", database="ytube_data")

    def get_mysql_connection(self):
        cnx = mysql.connector.Connect(user="root",password="admin", host="127.0.0.1", database="ytube_data")
        return cnx

    def execute_mysql_query(self, query):
        try:
            cnx = self.get_mysql_connection()
            with cnx.cursor() as cursor:
                result = cursor.execute(query)
                #rows = cursor.fetchall()
            cnx.commit()
            cnx.close()
            return "SUCCESS"
        except:
            return "FAILED"        

    def execute_mysql_query_with_values(self, query, values):
        try:
            cnx = self.get_mysql_connection()
            with cnx.cursor() as cursor:
                result = cursor.execute(query, values)
                #rows = cursor.fetchall()
            cnx.commit()
            cnx.close()
            return "SUCCESS"
        except:
            return "FAILED"              

    def load_channel_to_mysql(self, mongo_obj):
        #get channel details from mongo
        ch_df = mongo_obj.get_data_from_mongo("channel_details")
        #display(ch_df)
        print(ch_df.columns)
        print(ch_df.size)

        create_channel_query = """create table if not exists channels(channel_name varchar(200),
                                                                    channel_id varchar(100),
                                                                    channel_custom_url varchar(100),
                                                                    video_count int,
                                                                    view_count bigint,
                                                                    subscriber_count bigint,
                                                                    channel_description text,
                                                                    playlist_id varchar(100)
                                                                    ) """

        drop_channel_query = """drop table if exists channels"""                                                            
        op = self.execute_mysql_query(drop_channel_query)
        op = self.execute_mysql_query(create_channel_query)

        for index, row in ch_df.iterrows():
            insert_query="""insert into channels (channel_name
                                                    ,channel_id
                                                    ,channel_custom_url
                                                    ,video_count
                                                    ,view_count
                                                    ,subscriber_count
                                                    ,channel_description
                                                    ,playlist_id
                                                    ) 
                                                    values(%s, %s, %s, %s, %s, %s, %s, %s)"""
            values=(row['channel_name']
                    ,row['channel_id']
                    ,row['channel_custom_url']
                    ,row['video_count']
                    ,row['view_count']
                    ,row['subscriber_count']
                    ,row['channel_description']
                    ,row['playlist_id'])
            try:
                op = self.execute_mysql_query_with_values(insert_query, values)
                print(op)
            except:
                pass
        return "SUCCESS"                    



    def load_playlist_to_mysql(self, mongo_obj):
        #get playlist details from mongo
        ch_df = mongo_obj.get_data_from_mongo_with_array("playlist_details")
        print(ch_df.columns)
        print(ch_df.size)

        drop_query = """drop table if exists playlist"""    
        create_query = """create table if not exists playlist(playlist_id varchar(100),
                                                                    playlist_title varchar(200),
                                                                    channel_id varchar(100),
                                                                    channel_name varchar(200),
                                                                    published_at varchar(50),
                                                                    video_count int
                                                                    ) """

        op = self.execute_mysql_query(drop_query)
        op = self.execute_mysql_query(create_query)

        for index, row in ch_df.iterrows():
            insert_query="""insert into playlist (playlist_id
                                                    ,playlist_title
                                                    ,channel_id
                                                    ,channel_name
                                                    ,published_at
                                                    ,video_count
                                                    ) 
                                                    values(%s, %s, %s, %s, %s, %s)"""
            values=(row['playlist_id']
                    ,row['playlist_title']
                    ,row['channel_id']
                    ,row['channel_name']
                    ,row['published_at']
                    ,row['video_count'])
            try:
                op = self.execute_mysql_query_with_values(insert_query, values)
            except:
                pass
        return "SUCCESS"            


    def load_videos_to_mysql(self, mongo_obj):
        #get channel details from mongo
        ch_df = mongo_obj.get_data_from_mongo_with_array("video_details")
        print(ch_df.columns)
        print(ch_df.size)

        drop_query = """drop table if exists videos"""    
        create_query = """create table if not exists videos(video_title varchar(200),
                                                            video_id varchar(100),
                                                            channel_name varchar(200),
                                                            video_description text,
                                                            video_tags text,
                                                            video_thumbnail text,
                                                            video_view_count bigint,
                                                            video_like_count bigint,
                                                            video_comment_count bigint,
                                                            video_favorite_count bigint,
                                                            video_publishet_at  varchar(50),
                                                            video_duration  varchar(100),
                                                            video_definition  varchar(100),
                                                            video_caption  varchar(200)
                                                                    ) """

        op = self.execute_mysql_query(drop_query)
        op = self.execute_mysql_query(create_query)

        for index, row in ch_df.iterrows():
            insert_query="""insert into videos (video_title,
                                                video_id,
                                                channel_name,
                                                video_description,
                                                video_thumbnail,
                                                video_view_count,
                                                video_like_count,
                                                video_comment_count,
                                                video_favorite_count,
                                                video_publishet_at,
                                                video_duration,
                                                video_definition,
                                                video_caption
                                                ) 
                                                values(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"""
            values=(
                    row['video_title'],
                    row['video_id'],
                    row['channel_name'],
                    row['video_description'],
                    row['video_thumbnail'],
                    row['video_view_count'],
                    row['video_like_count'],
                    row['video_comment_count'],
                    row['video_favorite_count'],
                    row['video_publishet_at'],
                    row['video_duration'],
                    row['video_definition'],
                    row['video_caption'])
            try:
                op = self.execute_mysql_query_with_values(insert_query, values)
                if op == "FAILED":
                    print(insert_query)
                    print(values)
                    #break
            except:
                return "FAILED"
        return "SUCCESS"    


    def load_comments_to_mysql(self, mongo_obj):
        #get comments details from mongo
        ch_df = mongo_obj.get_data_from_mongo_with_array("comments_details")
        print(ch_df.columns)
        print(ch_df.size)       

        drop_query = """drop table if exists comments"""    
        create_query = """create table if not exists comments(comment_id varchar(100), 
                                                                video_id varchar(100), 
                                                                comment_text text, 
                                                                comment_author varchar(200), 
                                                                comment_published_at varchar(50)
                                                                    ) """

        op = self.execute_mysql_query(drop_query)
        op = self.execute_mysql_query(create_query)

        for index, row in ch_df.iterrows():
            insert_query="""insert into comments (comment_id, 
                                                video_id, 
                                                comment_text, 
                                                comment_author, 
                                                comment_published_at
                                                ) 
                                                values(%s, %s, %s, %s, %s)"""
            values=(
                    row['comment_id'], 
                    row['video_id'], 
                    row['comment_text'], 
                    row['comment_author'], 
                    row['comment_published_at'])
            try:
                op = self.execute_mysql_query_with_values(insert_query, values)
                if op == "FAILED":
                    print(insert_query)
                    print(values)
                    break
            except:
                return "FAILED"
        return "SUCCESS"            


    def get_data_from_mysql(self, query):
        try:
            cnx = self.get_mysql_connection()
            df = pd.read_sql(query, cnx)
            cnx.close()
            return df
        except:
            return "FAILED"        