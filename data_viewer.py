# -*- coding: utf-8 -*-
"""Module to run streamlit application to do youtube data analysis

Author      : Kannan Kandasamy
Draft       : Initial draft
Description : Module to show streamlit UI application to do youtube
data analytics
"""
import pandas as pd
import streamlit as st
from streamlit_option_menu import option_menu
import matplotlib.pyplot as plt
import altair as alt
from PIL import Image

from library.youtube_analysis import *
from library.mongod_load import *
from library.mysql_load import *

class DataViewer:
    """
    DataViewer uses streamlit and shows all UI options
    """
    if __name__=="__main__":
     
        mongo = Mongod()
        mys = Mysql()

        with st.sidebar:
            selected = option_menu(
                menu_title=None,
                options=["Home","Load Data","Youtube Data Analysis","Architecture","About"],
                default_index=0
            )

        if selected == "Home":
            st.title(f"Data Analytics")  
        if selected == "Architecture":
            st.write("Data product to get data from youtube using API, load into MongoDB and then into mysql db")
            image = Image.open('images/arch.drawio.png')
            st.image(image, caption="Architecture")            
        if selected == "About":
            st.title(f"About")                             
            st.write("This is Kannan Kandasamy")
            st.write("You can reach me at kannanvijay@hotmail.com")
            st.write("Stackoverflow https://stackoverflow.com/users/6466279/kannan-kandasamy")
            st.write("LinkedIn https://www.linkedin.com/in/kannankandasamy/")
        if selected == "Load Data":
            st.title("Load Data")
            st.write("Data loader for getting data from youtube and load into mongodb and mysql")
            channel_id_from_st = st.text_input('Channel Id','')
            if st.button('Load'):
                mys = Mysql()
                query = """select * from channels where channel_id = '{}';"""
                op_df = mys.get_data_from_mysql(query.format(channel_id_from_st))
                if len(op_df.index)==1:
                    st.write("This channel already loaded, please give valid channel_id to load")
                elif len(channel_id_from_st.strip())==0:
                    st.write("Channel_id is not valid, please give valid channel_id to load")
                else:
                    st.write("Load Started")
                    yd = Youtube()
                    youtube = yd.get_api_connection()  
                    st.write("Youtube object created successfully")
                    mongo = Mongod()
                    op = mongo.load_youtube_details_mongo(yd, channel_id_from_st)
                    st.write("Loaded into mongodb "+op)
                    st.write("------------------------------------------------")                    

                    op = mys.load_channel_to_mysql_alchemy(mongo)
                    st.write("Loaded Channels to Mysql "+op)

                    op = mys.load_playlist_to_mysql_alchemy(mongo)
                    st.write("Loaded Playlist to Mysql "+op)

                    op = mys.load_videos_to_mysql_alchemy(mongo)
                    st.write("Loaded Videos details to Mysql "+op)      

                    op = mys.load_comments_to_mysql_alchemy(mongo)
                    st.write("Loaded Comments to Mysql "+op)

                    st.write("------------------------------------------------")
                    st.write("Load to MongoDB and MySQL completed successfully")

        if selected == "Youtube Data Analysis":
            #st.title(f"You selected {selected}")     

            st.title("Youtube Data Analysis")
            query = """select question_id, question_name from questions order by question_id ;"""
            qn_df1 = mys.get_data_from_mysql(query)
            question_selected = st.sidebar.selectbox("Select Question", options = qn_df1['question_name'])            

            if question_selected.startswith("1."):
                #st.write("Select Channel")
                query = """select channel_name, channel_id from channels;"""
                chn_df1 = mys.get_data_from_mysql(query)
                channel_selected = st.sidebar.multiselect("Select Channel", options = chn_df1['channel_name'], default=list(chn_df1['channel_name'])[0])            
                #st.write(channel_selected)                
            
                st.write("1. Videos list for selected channel")   
                ch_selected="','".join(i for i in channel_selected)
                query = """select video_title as Video_Name, channel_name from videos where channel_name in ('{}');"""
                pl_df = mys.get_data_from_mysql(query.format(ch_selected))
                #print(chn_df)                
                st.dataframe(pl_df
                            ,hide_index=True,width=1200)     

                st.write("Number of videos per channel")
                query = """select channel_name, count(*) as number_of_videos from videos where channel_name in ('{}')
                            group by channel_name
                            order by channel_name;"""
                df1a = mys.get_data_from_mysql(query.format(ch_selected))
                #print(chn_df)                
                st.dataframe(df1a
                            ,hide_index=True,width=1200)     
                                            
            elif question_selected.startswith("2."):                
                st.write("2. Most Number videos")    
                query = """select channel_name, count(*) video_count from videos
                            group by channel_name
                            order by video_count desc limit 10;
                            """
                df2 = mys.get_data_from_mysql(query)
                #print(chn_df)                
                st.dataframe(df2
                            ,hide_index=True,width=1200)     

                st.write("In chart")
                fig1, ax1 = plt.subplots()
                ax1.pie(df2["video_count"], labels=df2["channel_name"])
                ax1.axis("equal")
                st.pyplot(fig1)

            elif question_selected.startswith("3."):
                st.write("3. Most viewed videos")    
                query = """select video_title as video_name, channel_name, video_view_count from videos
                            order by video_view_count desc
                            limit 10;"""
                df3 = mys.get_data_from_mysql(query)

                st.data_editor(
                    df3,
                    column_config={
                        "video_name": st.column_config.Column(
                            "Video Name",
                            width="large",
                        ),
                        "channel_name":st.column_config.Column(
                            "Channel Name",
                            width="small",
                        ),
                        "video_view_count":st.column_config.Column(
                            "View count",
                            width="small",
                        ),
                    },
                    use_container_width=True,
                    hide_index=True
                ) 

                st.write("In bar chart")
                #st.bar_chart(df3, x="video_name",y="video_view_count",color="video_name")
                st.write("In bar chart")
                c = alt.Chart(df3).mark_bar().encode(
                    x=alt.X('video_name', sort=None),
                    y='video_view_count',
                    color='video_name'
                )
                st.altair_chart(c, use_container_width=True)                

            elif question_selected.startswith("4."):
                st.write("4. How many comments on videos")    
                query = """
                    select video_title, channel_name, video_comments_count, channel_comments_count
                    from 
                    (
                        select video_title, channel_name, count(comment_id) over (partition by video_id) video_comments_count,
                        count(comment_id) over (partition by channel_name) as channel_comments_count, 
                        row_number() over(partition by channel_name, video_id order by comment_id) as rn
                        from 
                        (
                            select v.video_id, video_title, c.comment_id, v.channel_name
                            from videos v
                            join comments c
                            on v.video_id = c.video_id
                        ) a
                    ) b
                    where b.rn = 1;
                """
                df4 = mys.get_data_from_mysql(query)
                st.data_editor(
                    df4,
                    column_config={
                        "video_title": st.column_config.Column(
                            "Video Name",
                            width="large",
                        ),
                        "channel_name":st.column_config.Column(
                            "Channel Name",
                            width="small",
                        ),
                        "video_comments_count":st.column_config.Column(
                            "Comments count",
                            width="small",
                        ),
                        "channel_comments_count":st.column_config.Column(
                            "Channel Comments count",
                            width="small",
                        ),                        
                    },
                    use_container_width=True,
                    hide_index=True
                )                 
                st.write("Issue in commentThreads retrieval - retrieving only default 20, raised issue in issue tracker")
                st.write("https://issuetracker.google.com/issues/309981763")

            elif question_selected.startswith("5."):
                st.write("5. Highest Number of likes")    
                query = """select video_title as video_name, channel_name, video_like_count from videos
                            order by video_like_count desc
                            limit 10;"""
                df5 = mys.get_data_from_mysql(query)

                st.data_editor(
                    df5,
                    column_config={
                        "video_name": st.column_config.Column(
                            "Video Name",
                            width="large",
                        ),
                        "channel_name":st.column_config.Column(
                            "Channel Name",
                            width="small",
                        ),
                        "video_like_count":st.column_config.Column(
                            "Likes count",
                            width="small",
                        )
                    },
                    use_container_width=True,
                    hide_index=True
                ) 
                   
                
                st.write("In bar chart")
                c = alt.Chart(df5).mark_bar().encode(
                    x=alt.X('video_name', sort=None),
                    y='video_like_count',
                    color='video_name'
                )
                st.altair_chart(c, use_container_width=True)

            elif question_selected.startswith("6."):
                st.write("Youtube returns only likes count not the dislike counts")

            elif question_selected.startswith("7."):
                st.write("7. Total Number of views for a channel")    
                query = """select channel_name, view_count from channels
                            order by view_count desc limit 10;"""
                df7 = mys.get_data_from_mysql(query)
                #print(chn_df)                
                st.dataframe(df7
                            ,hide_index=True,width=1200)  

                st.write("In chart")
                fig1, ax1 = plt.subplots()
                ax1.pie(df7["view_count"], labels=df7["channel_name"])
                ax1.axis("equal")
                st.pyplot(fig1)

            elif question_selected.startswith("8."):
                year_selected=st.sidebar.selectbox("Select year", options = [2023,2022,2021,2020], index=1)
                st.write("8. Videos published on 2022 with channel names")    
                query = """select channel_name, count(*) as videos_on_year from videos where year(date(video_publishet_at))={}
                            group by channel_name
                            having count(*)>0;"""
                df8 = mys.get_data_from_mysql(query.format(year_selected))
                st.dataframe(df8,hide_index=True,use_container_width=True)   
                st.data_editor(
                    df8,
                    column_config={
                        "channel_name":"Channel Name",
                        "videos_on_year":st.column_config.ProgressColumn(
                            "Selected year Videos",
                            format="%f",
                            min_value=0,
                            max_value=300
                        ),
                    },
                    use_container_width=True,
                    hide_index=True
                )

                st.write("In bar chart")
                st.bar_chart(df8, x="channel_name",y="videos_on_year",color="channel_name")

            elif question_selected.startswith("9."):
                st.write("9. Average duration of videos in channels")    
                query = """select channel_name, round(avg(TIME_TO_SEC(STR_TO_DATE(video_duration, 'PT%iM%sS')))) as average_duration_seconds from videos
                            group by channel_name;"""
                df9 = mys.get_data_from_mysql(query)
                st.dataframe(df9
                            ,hide_index=True,width=1200)  

                st.write("In chart")
                fig1, ax1 = plt.subplots()
                ax1.pie(df9["average_duration_seconds"], labels=df9["channel_name"])
                ax1.axis("equal")
                st.pyplot(fig1)
                

            elif question_selected.startswith("10."):
                st.write("10. Highest number of comments for a channel")    
                query = """select v.channel_name as channel_name, count(*) as comments_count from videos v
                            join comments c
                            on v.video_id = c.video_id
                            group by v.channel_name
                            order by comments_count desc
                            limit 10;         """
                df10 = mys.get_data_from_mysql(query)
                st.dataframe(df10
                            ,hide_index=True,width=1200)   
                st.write("In bar chart")
                st.bar_chart(df10, x="channel_name",y="comments_count",color="channel_name")                
            else:
                st.write("Select valid option")