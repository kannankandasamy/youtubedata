import pandas as pd
import streamlit as st
from streamlit_option_menu import option_menu

from library.youtube_analysis import *
from library.mongod_load import *
from library.mysql_load import *

class DataViewer:
    if __name__=="__main__":
     
        mongo = Mongod()
        mys = Mysql()

        with st.sidebar:
            selected = option_menu(
                menu_title=None,
                options=["Home","Youtube Data Analysis","Load Data","About"],
                default_index=1
            )

        if selected == "Home":
            st.title(f"Data Analytics")     
        if selected == "About":
            st.title(f"About")                             
            st.write("This is Kannan Kandasamy")
            st.write("You can reach me at kannanvijay@hotmail.com")
            st.write("Stackoverflow https://stackoverflow.com/users/6466279/kannan-kandasamy")
        if selected == "Load Data":
            st.title("Load Data")
            st.write("Data loader for getting data from youtube and load into mongodb and mysql")
        if selected == "Youtube Data Analysis":
            #st.title(f"You selected {selected}")     

            st.title("Youtube Data Analysis")

            #st.write("Select Channel")
            query = """select channel_name, channel_id from channels;"""
            chn_df1 = mys.get_data_from_mysql(query)
            channel_selected = st.selectbox("Select Channel", options = chn_df1['channel_name'])            
            #st.write(channel_selected)

            st.write("1. Videos list for selected channel")    
            query = """select video_title as Video_Name from videos where channel_name = '{}';"""
            pl_df = mys.get_data_from_mysql(query.format(channel_selected))
            #print(chn_df)                
            st.dataframe(pl_df
                        ,hide_index=True,width=1200)      

            st.write("2. Most Number videos")    
            query = """select channel_name, count(*) video_count from videos
                        group by channel_name
                        order by video_count desc limit 10;
                        """
            df2 = mys.get_data_from_mysql(query)
            #print(chn_df)                
            st.dataframe(df2
                        ,hide_index=True,width=1200)                                    
        
            st.write("3. Most viewed videos")    
            query = """select video_title as vidoe_name, channel_name, video_view_count from videos
                        order by video_view_count desc
                        limit 10;"""
            df3 = mys.get_data_from_mysql(query)
            #print(chn_df)                
            st.dataframe(df3
                        ,hide_index=True,width=1200)       

            st.write("4. Most viewed videos")    
            query = """select v.video_title as video_name, count(*) as comments_count from videos v
                        join comments c
                        on v.video_id = c.video_id
                        group by v.video_title
                        order by v.video_title;"""
            df4 = mys.get_data_from_mysql(query)
            #print(chn_df)                
            st.dataframe(df4
                        ,hide_index=True,width=1200)   

            st.write("5. Highest Number of likes")    
            query = """select video_title as vidoe_name, channel_name, video_like_count from videos
                        order by video_like_count desc
                        limit 10;"""
            df5 = mys.get_data_from_mysql(query)
            #print(chn_df)                
            st.dataframe(df5
                        ,hide_index=True,width=1200)       


            st.write("7. Total Number of views for a channel")    
            query = """select channel_name, view_count from channels
                        order by view_count desc limit 10;"""
            df7 = mys.get_data_from_mysql(query)
            #print(chn_df)                
            st.dataframe(df7
                        ,hide_index=True,width=1200)      


            st.write("8. Videos published on 2022 with channel names")    
            query = """select channel_name, count(*) as videos_on_2022 from videos where year(date(video_publishet_at))=2022
                        group by channel_name
                        having count(*)>0;"""
            df8 = mys.get_data_from_mysql(query)
            #print(chn_df)                
            st.dataframe(df8
                        ,hide_index=True,width=1200)     


            st.write("9. Average duration of videos in channels")    
            query = """select channel_name, round(avg(TIME_TO_SEC(STR_TO_DATE(video_duration, 'PT%iM%sS')))) as average_duration_seconds from videos
                        group by channel_name;"""
            df9 = mys.get_data_from_mysql(query)
            #print(chn_df)                
            st.dataframe(df9
                        ,hide_index=True,width=1200)  


            st.write("10. Highest number of comments for a channel")    
            query = """select v.channel_name as channel_name, count(*) as comments_count from videos v
                        join comments c
                        on v.video_id = c.video_id
                        group by v.channel_name
                        order by comments_count desc
                        limit 10;         """
            df10 = mys.get_data_from_mysql(query)
            #print(chn_df)                
            st.dataframe(df10
                        ,hide_index=True,width=1200)   