from googleapiclient.discovery import build
import pymongo
import MySQLdb
import pandas as pd
import streamlit as st

def Api_connect():
    Api_id="AIzaSyBXf1NgPBCNC_ajXvNWZqfeZs2Tlfqpi6s"
    api_service_name="youtube"
    api_version="v3"
    youtube=build(api_service_name,api_version,developerKey=Api_id)
    return youtube

youtube=Api_connect()

def get_channel_info(channel_id):
    request=youtube.channels().list(
        part="snippet,ContentDetails,statistics",
        id=channel_id
    )
    response=request.execute()
    for i in response['items']:
        data=dict(Channel_Name=i["snippet"]["title"],
                Channel_id=i["id"],
                Subscribers=i["statistics"]["subscriberCount"],
                Tot_Views=i["statistics"]["viewCount"],
                Tot_Videos=i["statistics"]["videoCount"],
                Channel_desc=i["snippet"]["description"],
                Playlist_Id=i["contentDetails"]["relatedPlaylists"]["uploads"])
    return data

def video_ids(channel_id):
    video_ids=[]
    response=youtube.channels().list(id=channel_id,
                                    part='contentDetails'
                                    ).execute()
    Playlist_Id=response['items'][0]['contentDetails']['relatedPlaylists']['uploads']
    next_page_token=None

    while True:
        response1=youtube.playlistItems().list(
                                        part='snippet',
                                        playlistId=Playlist_Id,
                                        maxResults=50,
                                        pageToken=next_page_token).execute()
        for i in range(len(response1['items'])):
            video_ids.append(response1['items'][i]['snippet']['resourceId']['videoId'])
        next_page_token=response1.get('nextPageToken')

        if next_page_token is None:
            break
    return video_ids  

def get_video_info(video_IDs):
    video_data=[]
    for video_id in video_IDs:
        request=youtube.videos().list(
            part="snippet,ContentDetails,statistics",
            id=video_id
        )
        response=request.execute()    
        for item in response['items']:
            data=dict(Channel_Name=item['snippet']['channelTitle'],
                    Channel_Id=item['snippet']['channelId'],
                    Video_Id=item['id'],
                    Video_Title=item['snippet']['title'],
                    Tags=item['snippet'].get('tags'),
                    Thumbnail=item['snippet']['thumbnails']['default']['url'],
                    Description=item['snippet'].get('description'),
                    Published_Date=item['snippet']['publishedAt'],
                    Duration=item['contentDetails']['duration'],
                    Views=item['statistics'].get('viewCount'),
                    Likes=item['statistics'].get('likeCount'),
                    Comments=item['statistics'].get('commentCount'),
                    fav_count=item['statistics']['favoriteCount'],
                    Definition=item['contentDetails']['definition'],
                    Caption=item['contentDetails']['caption'])
            video_data.append(data)
    return video_data        
    
def get_comment_info(video_ids):
    Comment_data=[]
    try:
        for video_id in video_ids:
            request=youtube.commentThreads().list(
                part="snippet",
                videoId=video_id,
                maxResults=50,
            )
            response=request.execute()

            for item in response['items']:
                data=dict(Comment_Id=item['id'],
                video_idd=item['snippet']['topLevelComment']['snippet']['videoId'],
                Comment_Text=item['snippet']['topLevelComment']['snippet']['textDisplay'],
                Comment_Author=item['snippet']['topLevelComment']['snippet']['authorDisplayName'],
                Comment_Published_At=item['snippet']['topLevelComment']['snippet']['publishedAt']
                    )
                Comment_data.append(data)
    except:
        pass  
    return Comment_data 

client=pymongo.MongoClient("mongodb://localhost:27017/")
db=client["YouTube_data"]

def Channel_details(channel_id):
    ch_details=get_channel_info(channel_id)
    vi_ids=video_ids(channel_id)
    vi_details=get_video_info(vi_ids)
    cmt_details=get_comment_info(vi_ids)


    coll1=db["channel_details"]
    coll1.insert_one({"channel_information":ch_details,"video_information":vi_details,"comment_information":cmt_details})
    return "data load completed"

def channels_table():
  mydb = MySQLdb.connect(
    host="localhost",
    user="root",
    password="12345",
    database="Youtube_data"
  )
  mycursor = mydb.cursor()
  #mycursor.execute"CREATE DATABASE Youtube_data")
  mycursor = mydb.cursor()
  drop_query='''drop table if exists channels'''
  mycursor.execute(drop_query)
  mydb.commit()
  create_query='''create table channels(Channel_Name varchar(100),
                                        Channel_id varchar(100)primary key,Subscribers int,Tot_Views int,
                                        Tot_Videos int,Channel_desc text,Playlist_Id varchar(50))'''
  mycursor.execute(create_query)
  mydb.commit()
  ch_list=[]
  db=client["YouTube_data"]
  coll1=db["channel_details"]
  for ch_data in coll1.find({},{"_id":0,"channel_information":1}):
      ch_list.append(ch_data["channel_information"])
  df=pd.DataFrame(ch_list)
  for index,row in df.iterrows():
      insert_query='''insert into channels(Channel_Name,
                                          Channel_id,
                                          Subscribers,
                                          Tot_Views,
                                          Tot_Videos,
                                          Channel_desc,
                                          Playlist_Id)
                                          values(%s,%s,%s,%s,%s,%s,%s)'''
      values=(row['Channel_Name'],
              row['Channel_id'],
              row['Subscribers'],
              row['Tot_Views'],
              row['Tot_Videos'],
              row['Channel_desc'],
              row['Playlist_Id'])
      mycursor.execute(insert_query,values)     
      mydb.commit() 

import re
from datetime import datetime


# Function to convert ISO 8601 duration to total seconds
def iso8601_duration_to_seconds(duration):
    # Regular expression to extract hours, minutes, and seconds
    pattern = re.compile('P(?:(\d+)D)?T(?:(\d+)H)?(?:(\d+)M)?(?:(\d+)S)?')
    match = pattern.match(duration)
    if match:
        days, hours, minutes, seconds = [int(x) if x else 0 for x in match.groups()]
        total_seconds = days * 86400 + hours * 3600 + minutes * 60 + seconds
        return total_seconds
    else:
        # Handle the case where the duration does not match the pattern
        return 0  # or log an error
    
def convert_iso8601_to_mysql_datetime(iso8601_str):
    dt = datetime.strptime(iso8601_str, '%Y-%m-%dT%H:%M:%SZ')
    # If timezone handling is needed, use the following line instead
    # dt = datetime.strptime(iso8601_str, '%Y-%m-%dT%H:%M:%SZ').replace(tzinfo=pytz.UTC)
    return dt.strftime('%Y-%m-%d %H:%M:%S')

def video_table():
    mydb = MySQLdb.connect(
        host="localhost",
        user="root",
        password="12345",
        database="Youtube_data"
    )
    mycursor = mydb.cursor()

    drop_query = '''drop table if exists videos'''
    mycursor.execute(drop_query)
    mydb.commit()

    create_query = '''create table videos(Channel_Name varchar(100),
                                          Channel_Id varchar(100),
                                          Video_Id varchar(20) primary key,
                                          Video_Title varchar(200),
                                          Thumbnail varchar(200),
                                          Description text,
                                          Duration int,
                                          Views int,
                                          Likes int,
                                          Comments int,
                                          fav_count int,
                                          Definition varchar(10),
                                          Caption varchar(50),
                                          Published_Date DATETIME)'''
    mycursor.execute(create_query)
    mydb.commit()

    vi_list = []
    coll1 = db["channel_details"]
    for vi_data in coll1.find({}, {"_id": 0, "video_information": 1}):
        for i in range(len(vi_data["video_information"])):
            vi_list.append(vi_data["video_information"][i])

    df2 = pd.DataFrame(vi_list)
    for index, row in df2.iterrows():
        duration_seconds = iso8601_duration_to_seconds(row['Duration'])
        mysql_datetime = convert_iso8601_to_mysql_datetime(row['Published_Date'])

        insert_query = '''insert into videos(Channel_Name,
                                             Channel_Id,
                                             Video_Id,
                                             Video_Title,
                                             Thumbnail,
                                             Description,
                                             Duration,
                                             Views,
                                             Likes,
                                             Comments,
                                             fav_count,
                                             Definition,
                                             Caption,
                                             Published_Date)
                                             values(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)'''
        values = (row['Channel_Name'], row['Channel_Id'], row['Video_Id'], row['Video_Title'], row['Thumbnail'], row['Description'], duration_seconds, row['Views'], row['Likes'], row['Comments'], row['fav_count'], row['Definition'], row['Caption'], mysql_datetime)

        mycursor.execute(insert_query, values)
        mydb.commit()

def comments_table():
  mydb = MySQLdb.connect(
      host="localhost",
      user="root",
      password="12345",
      database="Youtube_data"
    )
  mycursor = mydb.cursor()
  #mycursor.execute("CREATE DATABASE Youtube_data")

  drop_query='''drop table if exists comments'''
  mycursor.execute(drop_query)
  mydb.commit()
  create_query='''create table comments(Comment_Id varchar(100) primary key,
                                      video_idd varchar(50),
                                      Comment_Text text,
                                      Comment_Author varchar(200),
                                      Comment_Published_At varchar(200))'''
  mycursor.execute(create_query)
  mydb.commit()
  cm_list=[]
  #db=client["YouTube_data"]
  coll1=db["channel_details"]
  for cm_data in coll1.find({},{"_id":0,"comment_information":1}):
    for i in range(len(cm_data["comment_information"])):
      cm_list.append(cm_data["comment_information"][i])
  df3=pd.DataFrame(cm_list) 
  for index,row in df3.iterrows():
        insert_query='''insert into comments(Comment_Id,
                                      video_idd,
                                      Comment_Text,
                                      Comment_Author,
                                      Comment_Published_At)
                                            values(%s,%s,%s,%s,%s)'''
        values=(row['Comment_Id'],
                row['video_idd'],
                row['Comment_Text'],
                row['Comment_Author'],
                row['Comment_Published_At'])
        mycursor.execute(insert_query,values)     
        mydb.commit() 


def fn_tables():
    channels_table()
    video_table()
    comments_table()
    return "Tables created successfully"   

def fn_show_channel():
    ch_list=[]
    db=client["YouTube_data"]
    coll1=db["channel_details"]
    for ch_data in coll1.find({},{"_id":0,"channel_information":1}):
        ch_list.append(ch_data["channel_information"])
    df=st.dataframe(ch_list)
    return df

def fn_show_video():
    vi_list=[]
    #db=client["YouTube_data"]
    coll1=db["channel_details"]
    for vi_data in coll1.find({},{"_id":0,"video_information":1}):
        for i in range(len(vi_data["video_information"])):
            vi_list.append(vi_data["video_information"][i])
    df2=st.dataframe(vi_list) 
    return df2

def fn_show_comments():
    cm_list=[]
    #db=client["YouTube_data"]
    coll1=db["channel_details"]
    for cm_data in coll1.find({},{"_id":0,"comment_information":1}):
        for i in range(len(cm_data["comment_information"])):
            cm_list.append(cm_data["comment_information"][i])
    df3=st.dataframe(cm_list) 
    return df3

#streamlit part
with st.sidebar:
    st.title(":green[YOUTUBE DATA HARVESTING AND WAREHOUSING]")
    st.header("YouTube Project")
    st.caption("Python Scrpting")
    st.caption("Data Collection")
    st.caption("MongDB")
    st.caption("API Integration")
    st.caption("Data Management using MongoDB and MySQL")

channel_id=st.text_input("Enter the Channel ID")
if st.button("Collect and Store Data"):
    ch_ids=[]
    db=client["YouTube_data"]
    coll1=db["channel_details"]
    for ch_data in coll1.find({},{"_id":0,"channel_information":1}):
        ch_ids.append(ch_data["channel_information"]["Channel_id"])

    if channel_id in ch_ids:
        st.success("Existing Channel")
    else:
        insert=Channel_details(channel_id)    
        st.success(insert)

if st.button("Migrate to SQL"):
    tables=fn_tables()
    st.success(tables)     

show_table=st.radio("SELECT THE TABLE FOR VIEW",("CHANNELS","VIDEOS","COMMENTS"))  
if show_table=="CHANNELS":
    fn_show_channel()
elif show_table=="VIDEOS":
    fn_show_video()
elif show_table=="COMMENTS":
    fn_show_comments()


#SQL Connection
mydb = MySQLdb.connect(
      host="localhost",
      user="root",
      password="12345",
      database="Youtube_data"
    )
mycursor = mydb.cursor()
question=st.selectbox("Select your question",("1. What are the names of all the videos and their corresponding channels?",
                                              "2. Which channels have the most number of videos, and how many videos do they have?",
                                              "3. What are the top 10 most viewed videos and their respective channels?",
                                              "4. How many comments were made on each video, and what are their corresponding video names?",
                                              "5. Which videos have the highest number of likes, and what are their corresponding channel names?",
                                              "6. What is the total number of likes and dislikes for each video, and what are their corresponding video names?",
                                              "7. What is the total number of views for each channel, and what are their corresponding channel names?",
                                              "8. What are the names of all the channels that have published videos in the year 2022?",
                                              "9. What is the average duration of all videos in each channel, and what are their corresponding channel names?",
                                              "10. Which videos have the highest number of comments, and what are their corresponding channel names?"
                                                 ))
                                                 

if question=="1. What are the names of all the videos and their corresponding channels?":
    query1='''select Video_Title as videos,Channel_Name as channelname from videos'''
    mycursor.execute(query1)
    mydb.commit()
    t1=mycursor.fetchall()
    df=pd.DataFrame(t1,columns=["video title","channel name"])
    st.write(df)    

elif question=="2. Which channels have the most number of videos, and how many videos do they have?":
  query2='''select Channel_Name as channelname, Tot_Videos as totvideos from channels order by Tot_Videos desc'''
  mycursor.execute(query2)
  mydb.commit()
  t2=mycursor.fetchall()
  df2=pd.DataFrame(t2,columns=["channel name","tot videos"])
  #df2
  st.write(df2)

elif question=="3. What are the top 10 most viewed videos and their respective channels?":
  query3='''select Views as totviews,Channel_Name as chnm,Video_Title as vtitle from videos where Views is not null order by Views desc limit 10'''
  mycursor.execute(query3)
  mydb.commit()
  t3=mycursor.fetchall()
  df3=pd.DataFrame(t3,columns=["Views","Channel Nm","Video title"])
  st.write(df3)

elif question=="4. How many comments were made on each video, and what are their corresponding video names?":
  query4='''select Comments as commtxt, Video_Title as vtitle from videos where Comments is not null'''
  mycursor.execute(query4)
  mydb.commit()
  t4=mycursor.fetchall()
  df4=pd.DataFrame(t4,columns=["Comments","Video title"])
  st.write(df4)  

elif question=="5. Which videos have the highest number of likes, and what are their corresponding channel names?":
  query5='''select Channel_Name as chlnm, Video_Title as vtitle,Likes as likes from videos where Likes is not null order by Likes desc'''
  mycursor.execute(query5)
  mydb.commit()
  t5=mycursor.fetchall()
  df5=pd.DataFrame(t5,columns=["Channel Name","Video title","Like count"])
  st.write(df5)  

elif question=="6. What is the total number of likes and dislikes for each video, and what are their corresponding video names?":
  query6='''select Likes as likes, Video_Title as vtitle from videos where Likes is not null order by Likes desc'''
  mycursor.execute(query6)
  mydb.commit()
  t6=mycursor.fetchall()
  df6=pd.DataFrame(t6,columns=["Like count","Video title"])
  st.write(df6)  

elif question=="7. What is the total number of views for each channel, and what are their corresponding channel names?":
  query7='''select  Channel_Name as Chnm,Tot_Views as totview from youtube_data.channels order by Tot_Views desc'''
  mycursor.execute(query7)
  mydb.commit()
  t7=mycursor.fetchall()
  df7=pd.DataFrame(t7,columns=["Channel Name","Tot Views"])
  st.write(df7)  

elif question=="8. What are the names of all the channels that have published videos in the year 2022?":
  query8='''SELECT distinct(Channel_Name) FROM videos where year(Published_Date)=2022'''
  mycursor.execute(query8)
  mydb.commit()
  t8=mycursor.fetchall()
  df8=pd.DataFrame(t8,columns=["Channel Name"])
  st.write(df8)   

elif question=="9. What is the average duration of all videos in each channel, and what are their corresponding channel names?":
  query9='''SELECT avg(Duration) as averag_in_sec,Channel_Name FROM videos group by Channel_Name'''
  mycursor.execute(query9)
  mydb.commit()
  t9=mycursor.fetchall()
  df9=pd.DataFrame(t9,columns=["Avg_In_Sec","Channel Name"])
  st.write(df9)  


elif question=="10. Which videos have the highest number of comments, and what are their corresponding channel names?":
  query10='''select Comments as cmt,Video_Title as vidtit,Channel_Name as chnm from youtube_data.videos where Comments is not null order by Comments desc'''
  mycursor.execute(query10)
  mydb.commit()
  t10=mycursor.fetchall()
  df10=pd.DataFrame(t10,columns=["Comment","Video Title","Channel Name"])
  st.write(df10)         
