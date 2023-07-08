from datetime import datetime
import altair as alt
import isodate
import matplotlib.pyplot as plt
import mysql
import mysql.connector
import pandas as pd
import plotly.express as px
import pymongo
import pymysql
import sqlalchemy
import streamlit as st
from pymongo import MongoClient
from sqlalchemy import create_engine
from streamlit_extras.add_vertical_space import add_vertical_space
from wordcloud import WordCloud
from mysql.connector.plugins import caching_sha2_password
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

dataFrameSerialization = "legacy"
def add_bg_from_url():
    st.markdown(
         f"""
         <style>
         .stApp {{
             background-image: url("https://cdn.pixabay.com/photo/2019/04/24/11/27/flowers-4151900_960_720.jpg");
             background-attachment: fixed;
             background-size: cover
         }}
         </style>
         """,
         unsafe_allow_html=True
     )

add_bg_from_url()
# ------------------------API SETUP----------------------------------------------#
api_key = "AIzaSyAmwRZ9njd9CHKOZ167LY9KMVD2rJML2Ck"

youtube = build('youtube', 'v3', developerKey=api_key)

request = youtube.channels().list(
    part="snippet,contentDetails,statistics",
    id="UC_x5XG1OV2P6uZZ5FSM9Ttw"
)
response = request.execute()



# ------------------------------------PAGE SETUP------------------------------#
st.header(':red[Welcome to YouTube Harvesting!]')
col1, buff = st.columns([3, 7])
add_vertical_space(2)
channel_username = col1.text_input(':blue[Enter channel username]')

# ---------------------------------MongoDb Connection Set up-------------------------------------------#

# MongoDB connection

mongo_client = MongoClient("mongodb://localhost:27017")
db = mongo_client['youtubeharvesting']
collection = db['youtubeharvetsing_data']


# -----------------------REQUIRED FUNCTIONS TO EXTRACT DATA FROM THE API RESPONSE-----------------------#

# Function for Datetime Formatting
def convert_datetime(published_at):
    datetime_obj = datetime.strptime(published_at, "%Y-%m-%dT%H:%M:%SZ")
    return datetime_obj.strftime('%Y-%m-%d %H:%M:%S')


# Function for formatting the Duration of the videos
def format_duration(duration):
    duration_obj = isodate.parse_duration(duration)
    hours = duration_obj.total_seconds() // 3600
    minutes = (duration_obj.total_seconds() % 3600) // 60
    seconds = duration_obj.total_seconds() % 60

    formatted_duration = f"{int(hours):02d}:{int(minutes):02d}:{int(seconds):02d}"
    return formatted_duration


# Function to get the channel ids

def get_channel_id(api_key, channel_username):
    try:
        youtube = build('youtube', 'v3', developerKey=api_key)
        response = youtube.search().list(
            part='snippet',
            q=channel_username,
            type="channel",
            maxResults=1
        ).execute()

        if 'items' in response and response['items']:
            channel_item = response['items'][0]
            return channel_item['snippet']['channelId']
        elif 'items' not in response:
            st.write(f"The Channel Info is Invalid:")
            st.error(f"Please verify the Channel Info entered!")
    except HttpError as e:
        if e.resp.status == 403 and b"quotaExceeded" in e.content:
            st.write("API Quota exhausted... Try using after 24 hours")
        else:
            raise Exception('Channel ID not found.')


# FUNCTION TO FETCH VIDEO COMMENTS
def fetch_video_comments(youtube, video_id, max_results=3):
    try:
        comments_response = youtube.commentThreads().list(
            part='snippet',
            videoId=video_id,
            maxResults=max_results).execute()
    except HttpError as e:
        if e.resp.status == 403:
            return {}
        else:
            raise
    comments = comments_response['items']

    video_comments = {}

    for idx, comment in enumerate(comments):
        comment_id = comment['snippet']['topLevelComment']['id']
        comment_text = comment['snippet']['topLevelComment']['snippet']['textDisplay']
        comment_author = comment['snippet']['topLevelComment']['snippet']['authorDisplayName']
        comment_published_at = convert_datetime(comment['snippet']['topLevelComment']['snippet']['publishedAt'])

        video_comments[f'Comment_{idx + 1}'] = {
            'Comment_Id': comment_id,
            'Comment_Text': comment_text,
            'Comment_Author': comment_author,
            'Comment_PublishedAt': comment_published_at
        }

    return video_comments


channel_name = None


# FUNCTION TO FETCH CHANNEL DATA

def fetch_channel_data(api_key, channel_id):
    global channel_name
    status_text = st.empty()
    try:
        youtube = build('youtube', 'v3', developerKey=api_key)

        channel_response = youtube.channels().list(
            part='snippet, statistics, contentDetails, status',
            id=channel_id).execute()

        channel_items = channel_response.get('items', [])
        if channel_items:
            channel_item = channel_items[0]
            channel_name = channel_item['snippet']['title']
            st.session_state['channel_name'] = channel_name
            subscription_count = int(channel_item['statistics']['subscriberCount'])
            view_count = int(channel_item['statistics']['viewCount'])
            if channel_item['snippet']['description'] == '':
                channel_description = 'NA'
            else:
                channel_description = channel_item['snippet']['description']
            uploads_playlist_id = channel_item['contentDetails']['relatedPlaylists']['uploads']
            channel_status = channel_item['status']['privacyStatus']
        else:
            channel_name = 'NA'
            subscription_count = 0
            view_count = 0
            channel_description = 'NA'
            uploads_playlist_id = 'NA'
            channel_status = 'NA'

        playlists = []
        next_page_token = None

        while True:
            playlists_response = youtube.playlists().list(
                part='snippet',
                channelId=channel_id,
                maxResults=50,
                pageToken=next_page_token
            ).execute()

            playlists.extend(playlists_response.get('items', []))

            next_page_token = playlists_response.get('nextPageToken')

            if next_page_token is None:
                break

        video_details = {}
        video_index = 1
        added_video_ids = set()

        for playlist in playlists:
            playlist_id = playlist['id']
            playlist_name = playlist['snippet']["title"]

            next_page_token = None
            videos = []

            while True:
                playlist_items_response = youtube.playlistItems().list(
                    part='snippet',
                    playlistId=playlist_id,
                    maxResults=50,
                    pageToken=next_page_token
                ).execute()

                videos.extend(playlist_items_response.get('items', []))

                next_page_token = playlist_items_response.get('nextPageToken')

                if next_page_token is None:
                    break

            for item in videos:
                video_id = item['snippet']['resourceId']['videoId']

                if video_id in added_video_ids:
                    continue

                video_response = youtube.videos().list(
                    part='snippet,contentDetails,statistics',
                    id=video_id
                ).execute()
                video_items = video_response.get('items', [])

                if video_items:
                    video_snippet = video_items[0]['snippet']
                    video_stats = video_items[0]['statistics']
                    video_name = video_snippet['title']
                    video_description = video_snippet['description']
                    video_tags = video_snippet.get('tags', [])
                    published_at = convert_datetime(video_snippet['publishedAt'])
                    view_count = int(video_stats.get('viewCount', 0))
                    like_count = int(video_stats.get('likeCount', 0))
                    dislike_count = int(video_stats.get('dislikeCount', 0))
                    favorite_count = int(video_stats.get('favoriteCount', 0))
                    comment_count = int(video_stats.get('commentCount', 0))
                    duration = format_duration(video_items[0]['contentDetails']['duration'])
                    thumbnail = video_snippet['thumbnails']['default']['url']
                    caption_status = video_snippet.get('caption', 'Not available')
                else:
                    continue

                video_key = f'Video_{video_index}'
                video_comments = fetch_video_comments(youtube, video_id)

                video_details[video_key] = {
                    'Playlist_Id': playlist_id,
                    'Video_Id': video_id,
                    'Playlist_Name': playlist_name,
                    'Video_Name': video_name,
                    'Video_Description': video_description,
                    'Tags': video_tags,
                    'PublishedAt': published_at,
                    'View_Count': view_count,
                    'Like_Count': like_count,
                    'Dislike_Count': dislike_count,
                    'Favorite_Count': favorite_count,
                    'Comment_Count': comment_count,
                    'Duration': duration,
                    'Thumbnail': thumbnail,
                    'Caption_Status': caption_status,
                    'Comments': video_comments
                }

                added_video_ids.add(video_id)
                video_index += 1

        next_page_token = None
        remaining_videos = []

        while True:
            remaining_videos_response = youtube.search().list(
                part='snippet',
                channelId=channel_id,
                maxResults=50,
                type='video',
                pageToken=next_page_token
            ).execute()

            remaining_videos.extend(remaining_videos_response.get('items', []))

            next_page_token = remaining_videos_response.get('nextPageToken')

            if next_page_token is None:
                break

        for item in remaining_videos:
            video_id = item['id']['videoId']

            if video_id in added_video_ids:
                continue

            video_response = youtube.videos().list(
                part='snippet,contentDetails,statistics',
                id=video_id
            ).execute()
            video_items = video_response.get('items', [])

            if video_items:
                video_snippet = video_items[0]['snippet']
                video_stats = video_items[0]['statistics']
                video_name = video_snippet['title']
                video_description = video_snippet['description']
                video_tags = video_snippet.get('tags', [])
                published_at = convert_datetime(video_snippet['publishedAt'])
                view_count = int(video_stats.get('viewCount', 0))
                like_count = int(video_stats.get('likeCount', 0))
                dislike_count = int(video_stats.get('dislikeCount', 0))
                favorite_count = int(video_stats.get('favoriteCount', 0))
                comment_count = int(video_stats.get('commentCount', 0))
                duration = format_duration(video_items[0]['contentDetails']['duration'])
                thumbnail = video_snippet['thumbnails']['default']['url']
                caption_status = video_snippet.get('caption', 'Not available')
            else:
                continue

            video_key = f'Video_{video_index}'
            video_comments = fetch_video_comments(youtube, video_id)

            video_details[video_key] = {
                'Playlist_Id': 'NA',
                'Video_Id': video_id,
                'Playlist_Name': 'NA',
                'Video_Name': video_name,
                'Video_Description': video_description,
                'Tags': video_tags,
                'PublishedAt': published_at,
                'View_Count': view_count,
                'Like_Count': like_count,
                'Dislike_Count': dislike_count,
                'Favorite_Count': favorite_count,
                'Comment_Count': comment_count,
                'Duration': duration,
                'Thumbnail': thumbnail,
                'Caption_Status': caption_status,
                'Comments': video_comments
            }

            added_video_ids.add(video_id)
            video_index += 1

        channel_details = {
            'Channel_Id': channel_id,
            'Channel_Name': channel_name,
            'Uploads_Playlist_Id': uploads_playlist_id,
            'Subscription_Count': subscription_count,
            'Channel_Views': view_count,
            'Channel_Description': channel_description,
            'Channel_Status': channel_status
        }

        data = {
            '_id': channel_id,
            'Channel_Details': channel_details,
            'Video_Details': video_details
        }

        return data

    except HttpError as e:
        if e.resp.status == 403 and b"quotaExceeded" in e.content:
            status_text.write("API Quota exhausted... Try using after 24 hours")
        else:
            raise Exception('API request broke...Try again...')


but, ref = st.columns(2)

if but.button('FETCH DATA AND PUSH TO MongoDB', key='push'):
    channel_id = get_channel_id(api_key, channel_username)
    channel_data = fetch_channel_data(api_key, channel_id)

    channel_details = channel_data['Channel_Details']
    df = pd.DataFrame.from_dict([channel_details]).rename(columns={
        "Channel_Name": "Channel Name",
        "Channel_Id": "Channel ID",
        "Uploads_Playlist_Id": "Channel Playlist ID",
        "Subscription_Count": "Subscription Count",
        "Channel_Views": "Channel View Count",
        "Channel_Description": "Channel Description",
        "Channel_Status": "Channel Status"
    }
    )
    df.index = [1]
    st.dataframe(df)

    existing_doc = collection.find_one({"_id": channel_data["_id"]})

    if existing_doc:
        collection.replace_one({"_id": channel_data["_id"]}, channel_data)
    else:
        collection.insert_one(channel_data)

    st.write("Data Fetched Successfully!")


# FUNCTION To FETCH DOCUMENTS

def fetch_document(collection, channel_name):
    document = collection.find_one({"Channel_Details.Channel_Name": channel_name})
    return document


def fetch_channel_names(collection):
    channel_names = collection.distinct("Channel_Details.Channel_Name")
    return channel_names


if "channel_names" not in st.session_state:
    channel_names = fetch_channel_names(collection)
    st.session_state["channel_names"] = channel_names

existing_channel_count = len(st.session_state["channel_names"])
new_channel_count = collection.estimated_document_count()

if existing_channel_count != new_channel_count:
    existing_channels = set(st.session_state["channel_names"])
    new_channel_names = [name for name in fetch_channel_names(collection) if name not in existing_channels]
    st.session_state["channel_names"].extend(new_channel_names)


# FUNCTION TO FETCH VIDEO DATA


def fetch_video_dataframe(document):
    video_details = document["Video_Details"]

    video_df_data = []
    for video_key, video_info in video_details.items():
        video_df_entry = {
            "Video_Name": video_info.get("Video_Name", ""),
            "Playlist_Id": video_info.get("Playlist_Id", ""),
            "Playlist_Name": video_info.get("Playlist_Name", ""),
            "PublishedAt": video_info.get("PublishedAt", ""),
            "View_Count": video_info.get("View_Count", ""),
            "Like_Count": video_info.get("Like_Count", ""),
            "Dislike_Count": video_info.get("Dislike_Count", ""),
            "Favorite_Count": video_info.get("Favorite_Count", ""),
            "Comment_Count": video_info.get("Comment_Count", ""),
            "Duration": video_info.get("Duration", "")
        }
        video_df_data.append(video_df_entry)

    video_df = pd.DataFrame(video_df_data)
    return video_df


add_vertical_space(2)

st.subheader("Details of the Videos!")

add_vertical_space(3)

col1, col2, col3 = st.columns(3)
selected_channel = col1.selectbox("Select Channel", list(st.session_state["channel_names"]), key="channel")

if selected_channel:

    selected_document = fetch_document(collection, selected_channel)

    if selected_document:

        selected_video_df = fetch_video_dataframe(selected_document)

        if not selected_video_df.empty:

            selected_video_df = selected_video_df[
                ["Video_Name", "Playlist_Name", "PublishedAt", "View_Count", "Like_Count", "Dislike_Count",
                 "Favorite_Count", "Comment_Count", "Duration"]]

            playlist_names = selected_video_df["Playlist_Name"].unique()
            playlist_names = [playlist for playlist in playlist_names if playlist != "NA"]

            videos_not_in_playlist = selected_video_df[selected_video_df["Playlist_Name"] == "NA"]

            if not videos_not_in_playlist.empty:
                playlist_names = ["Videos not in Playlists"] + playlist_names

            selected_playlist = col2.selectbox("Select Playlist", playlist_names, key="playlist")

            if selected_playlist:

                if selected_playlist == "Videos not in Playlists":
                    filtered_videos = videos_not_in_playlist
                else:
                    filtered_videos = selected_video_df[selected_video_df["Playlist_Name"] == selected_playlist]

                if not filtered_videos.empty:

                    video_names = filtered_videos["Video_Name"].tolist()
                    video_names.insert(0, "Select Video")

                    selected_video = col3.selectbox("Select Video", video_names, key="video")

                    if selected_video != "Select Video":

                        video_details = filtered_videos[filtered_videos["Video_Name"] == selected_video].iloc[0]
                        video_details = pd.DataFrame(video_details)
                        video_details.columns = ["Details"]
                        video_details.rename({
                            "Video_Name": "Video Name", "Playlist_Name": "Playlist Name",
                            "PublishedAt": "Published at", "View_Count": "View Count",
                            "Like_Count": "Like Count", "Dislike_Count": "Dislike Count",
                            "Favorite_Count": "Favorite Count", "Comment_Count": "Comment Count",
                        }, axis=0, inplace=True)

                        st.dataframe(video_details, width=600)
                    else:
                        st.write("")
                else:
                    st.write("No videos available for the selected playlist.")
            else:
                st.write("Select a playlist.")
        else:
            st.write("No videos available for the selected channel.")
    else:
        st.write("Selected channel document not found.")
else:
    st.write("Select a channel.")
# Streamlit Settings
side_bar = st.sidebar

side_bar.image("https://cdn.pixabay.com/photo/2019/04/24/11/27/flowers-4151900_960_720.jpg")
side_bar.subheader('Youtube Data Analysis!')
selected_channel = side_bar.selectbox("Select Channel", st.session_state['channel_names'], key='channels')

channel_data = collection.find_one({"Channel_Details.Channel_Name": selected_channel})
video_details = channel_data["Video_Details"]

viz_options = ['Animated Bubble Plot', 'Word Cloud', 'Donut Chart', 'Bar Chart']

if 'viz_options' not in st.session_state:
    st.session_state['viz_options'] = viz_options

selected_viz = side_bar.selectbox("Select Visualization", st.session_state['viz_options'], key='selected_viz')
viz_button = side_bar.button("Show Visualization", key='viz')

if selected_viz == 'Animated Bubble Plot' and viz_button:

    st.subheader(f'Video view count trend in {selected_channel} channel')
    data = []

    for video_key, video_data in video_details.items():
        view_count = video_data.get('View_Count', 0)
        published_time = video_data.get('PublishedAt', '')

        published_time = pd.to_datetime(published_time)

        data.append({'View_Count': view_count, 'Published_Time': published_time, 'Video_Key': video_key})

    df = pd.DataFrame(data)

    df = df.sort_values('Published_Time')

    df['Video Number'] = range(1, len(df) + 1)

    fig = px.scatter(df, x='Published_Time', y='View_Count', size='View_Count', animation_frame='Video Number',
                     range_x=[df['Published_Time'].min(), df['Published_Time'].max()],
                     range_y=[df['View_Count'].min(), df['View_Count'].max()])

    fig.update_layout(xaxis_title='Published Time', yaxis_title='View Count')

    frame_duration = 1000

    st.plotly_chart(fig, use_container_width=True,
                    config={'plotly': {'animation': {'frame': {'duration': frame_duration}}}})

elif selected_viz == 'Word Cloud' and viz_button:

    st.subheader(f'Video Titles for {selected_channel} channel')

    video_titles = [video_details[key]['Video_Name'] for key in video_details]
    text = ' '.join(video_titles)

    wordcloud = WordCloud(width=800, height=400, background_color='white').generate(text)

    plt.figure(figsize=(10, 5))
    plt.imshow(wordcloud, interpolation='bilinear')
    plt.axis('off')
    st.pyplot(plt, use_container_width=True)

elif selected_viz == 'Donut Chart' and viz_button:

    st.subheader(f'Top Playlists by view count in {selected_channel} channel')

    playlist_views = {}

    for video_key in video_details:
        playlist_name = video_details[video_key]['Playlist_Name']
        if playlist_name == 'NA':
            continue
        view_count = video_details[video_key]['View_Count']
        if playlist_name not in playlist_views:
            playlist_views[playlist_name] = view_count
        else:
            playlist_views[playlist_name] += view_count

    sorted_playlists = sorted(playlist_views, key=lambda x: playlist_views[x], reverse=True)
    sorted_counts = [playlist_views[x] for x in sorted_playlists]

    top_playlists = sorted_playlists[:6]
    other_count = sum(sorted_counts[6:])
    top_counts = sorted_counts[:6] + [other_count]

    data = pd.DataFrame({'Playlist': top_playlists + ['Others'], 'View_Count': top_counts})

    fig = px.pie(data, values='View_Count', names='Playlist', hole=0.6)
    fig.update_traces(textposition='inside', textinfo='percent')

    fig.update_layout(
        showlegend=True,
        legend_title='Playlist',
        height=500,
        width=800
    )

    playlist_counts = [len([v for v in video_details.values() if v['Playlist_Name'] == playlist]) for playlist in
                       top_playlists]
    playlist_counts.append(len([v for v in video_details.values() if v['Playlist_Name'] not in top_playlists]))
    fig.update_traces(
        hovertemplate='<b>%{label}</b><br>View Count: %{value}<br>Number of Videos: %{text}<extra></extra>',
        text=playlist_counts)

    st.plotly_chart(fig, use_container_width=True)

elif selected_viz == 'Bar Chart' and viz_button:

    st.subheader(f'Top 10 Videos by like counts in {selected_channel} channel')

    top_videos = sorted(video_details.keys(), key=lambda x: video_details[x]['Like_Count'], reverse=True)[:10]

    data = pd.DataFrame({'Video Name': [video_details[key]['Video_Name'] for key in top_videos],
                         'Like Count': [video_details[key]['Like_Count'] for key in top_videos]})

    axis_format = '~s'

    chart = alt.Chart(data).mark_bar(size=18).encode(
        x=alt.X(
            "Like Count",
            axis=alt.Axis(format=axis_format)
        ),
        y=alt.Y(
            "Video Name",
            sort='-x',
            title=None
        ),
        tooltip=[
            'Video Name', 'Like Count'
        ]
    ).properties(width=600, height=400).configure_axis(grid=False)

    st.altair_chart(chart, use_container_width=True)

# ======================================== Data Migrate zone (Stored data to MySQL)========================================== #


st.header(':violet[Data Migrate zone]')
st.write('''(Note:- This zone specific channel data **Migrate to :blue[MySQL] database from  :green[MongoDB] database** depending on your selection,
                if unavailable your option first collect data.)''')

# Connect to the MongoDB server
client = pymongo.MongoClient("mongodb://localhost:27017/")
mydb = client['youtubeharvesting_data']
document_names = []

for document in collection.find():
    document_names.append(document["Channel_Details"]["Channel_Name"])

document_name = st.selectbox('Select channel name', options=document_names, key='document_name')
st.write('''Migrate to MySQL database from MongoDB database to click below **:blue['Migrate to MySQL']**.''')
Migrate = st.button('**Migrate to MySQL**')

if 'migrate_sql' not in st.session_state:
    st.session_state_migrate_sql = False
if Migrate or st.session_state_migrate_sql:
    st.session_state_migrate_sql = True


result = collection.find_one({"Channel_Details.Channel_Name":document_name})
# st.write(result)
# ----------------------------- Data conversion --------------------- #

# print(result['Channel_Details']['Channel_Name'])
# Channel data json to df

channel_details_to_sql = {
    "Channel_Name": result['Channel_Details']['Channel_Name'],
    "Channel_Id": result['_id'],
    # "Video_Count": result['Video_Details'][''],
    "Subscriber_Count": result['Channel_Details']['Subscription_Count'],
    "Channel_Views": result['Channel_Details']['Channel_Views'],
    "Channel_Description": result['Channel_Details']['Channel_Description'],
    "Playlist_Id": result['Video_Details']['Video_1']['Playlist_Id']}

channel_df = pd.DataFrame.from_dict(channel_details_to_sql, orient='index').T

# playlist data json to df
playlist_tosql = {"Channel_Id": result['_id'],
                  "Playlist_Id": result['Video_Details']['Video_1']['Playlist_Id']
                  }
playlist_df = pd.DataFrame.from_dict(playlist_tosql, orient='index').T

# video data json to df
video_details_list = []
for i in range(1, len(result['Video_Details']) - 1):
    video_details_tosql = {
        'Playlist_Id': result['Video_Details'][f"Video_{i}"]['Playlist_Id'],
        'Video_Id': result['Video_Details'][f"Video_{i}"]['Video_Id'],
        'Video_Name': result['Video_Details'][f"Video_{i}"]['Video_Name'],
        'Video_Description': result['Video_Details'][f"Video_{i}"]['Video_Description'],
        'Published_date': result['Video_Details'][f"Video_{i}"]['PublishedAt'],
        'View_Count': result['Video_Details'][f"Video_{i}"]['View_Count'],
        'Like_Count': result['Video_Details'][f"Video_{i}"]['Like_Count'],
        'Dislike_Count': result['Video_Details'][f"Video_{i}"]['Dislike_Count'],
        'Favorite_Count': result['Video_Details'][f"Video_{i}"]['Favorite_Count'],
        'Comment_Count': result['Video_Details'][f"Video_{i}"]['Comment_Count'],
        'Duration': result['Video_Details'][f"Video_{i}"]['Duration'],
        'Thumbnail': result['Video_Details'][f"Video_{i}"]['Thumbnail'],
        'Caption_Status': result['Video_Details'][f"Video_{i}"]['Caption_Status']}
    video_details_list.append(video_details_tosql)
    video_df = pd.DataFrame(video_details_list)

# Comment data json to df
Comment_details_list = []
for i in range(1, len(result['Video_Details']['Video_1']['Comments']) - 1):
    comments_access = result['Video_Details']['Video_1']['Comments']
    if comments_access == 'Unavailable' or (
            'Comment_1' not in comments_access or 'Comment_2' not in comments_access):
        Comment_details_tosql = {
            'Video_Id': 'Unavailable',
            'Comment_Id': 'Unavailable',
            'Comment_Text': 'Unavailable',
            'Comment_Author': 'Unavailable',
            'Comment_Published_date': 'Unavailable', }
        Comment_details_list.append(Comment_details_tosql)
    else:
        for j in range(1, 3):
            Comment_details_tosql = {
                'Video_Id': result['Video_Details'][f"Video_{i}"]['Video_Id'],
                'Comment_Id': result['Video_Details'][f"Video_{i}"]['Comments'][f"Comment_{j}"][
                    'Comment_Id'],
                'Comment_Text': result['Video_Details'][f"Video_{i}"]['Comments'][f"Comment_{j}"][
                    'Comment_Text'],
                'Comment_Author': result['Video_Details'][f"Video_{i}"]['Comments'][f"Comment_{j}"][
                    'Comment_Author'],
                'Comment_Published_date':
                    result['Video_Details'][f"Video_{i}"]['Comments'][f"Comment_{j}"][
                        'Comment_PublishedAt'],
            }
        Comment_details_list.append(Comment_details_tosql)
        Comments_df = pd.DataFrame(Comment_details_list)

# -------------------- Data Migrate to MySQL --------------- #
        mydatabase = mysql.connector.connect(
                host="localhost",
                user="root",
                password="mp141534",
                database = "youtube_harvesting",
                auth_plugin="mysql_native_password")
        cursor = mydatabase.cursor()
        cursor.execute("CREATE DATABASE IF NOT EXISTS youtube_harvesting")
        cursor.close()
        mydatabase.close()


# Connect to the new created database
        engine = create_engine('mysql+mysqlconnector://root:mp141534@localhost/youtube_harvesting', echo=False)

# Use pandas to insert the DataFrames data to the SQL Database -> table1

# Channel data to SQL
        channel_df.to_sql('channel', engine, if_exists='append', index=False,
                  dtype={"Channel_Name": sqlalchemy.types.VARCHAR(length=225),
                         "Channel_Id": sqlalchemy.types.VARCHAR(length=225),
                         "Video_Count": sqlalchemy.types.INT,
                         "Subscriber_Count": sqlalchemy.types.BigInteger,
                         "Channel_Views": sqlalchemy.types.BigInteger,
                         "Channel_Description": sqlalchemy.types.TEXT,
                         "Playlist_Id": sqlalchemy.types.VARCHAR(length=225), })

# Playlist data to SQL
        playlist_df.to_sql('playlist', engine, if_exists='append', index=False,
                   dtype={"Channel_Id": sqlalchemy.types.VARCHAR(length=225),
                          "Playlist_Id": sqlalchemy.types.VARCHAR(length=225), })

# Video data to SQL
        video_df.to_sql('video', engine, if_exists='append', index=False,
                dtype={'Playlist_Id': sqlalchemy.types.VARCHAR(length=225),
                       'Video_Id': sqlalchemy.types.VARCHAR(length=225),
                       'Video_Name': sqlalchemy.types.VARCHAR(length=225),
                       'Video_Description': sqlalchemy.types.TEXT,
                       'Published_date': sqlalchemy.types.String(length=50),
                       'View_Count': sqlalchemy.types.BigInteger,
                       'Like_Count': sqlalchemy.types.BigInteger,
                       'Dislike_Count': sqlalchemy.types.INT,
                       'Favorite_Count': sqlalchemy.types.INT,
                       'Comment_Count': sqlalchemy.types.INT,
                       'Duration': sqlalchemy.types.VARCHAR(length=1024),
                       'Thumbnail': sqlalchemy.types.VARCHAR(length=225),
                       'Caption_Status': sqlalchemy.types.VARCHAR(length=225), })

# Commend data to SQL
        Comments_df.to_sql('comments', engine, if_exists='append', index=False,
                   dtype={'Video_Id': sqlalchemy.types.VARCHAR(length=225),
                          'Comment_Id': sqlalchemy.types.VARCHAR(length=225),
                          'Comment_Text': sqlalchemy.types.TEXT,
                          'Comment_Author': sqlalchemy.types.VARCHAR(length=225),
                          'Comment_Published_date': sqlalchemy.types.String(length=50), })

# ====================================================   /     Channel Analysis zone     /   ================================================= #

st.header(':violet[Channel Data Analysis zone]')

engine = create_engine('mysql+mysqlconnector://root:mp141534@localhost/youtube_harvesting', echo=False)
query = "SELECT distinct Channel_Name FROM channel;"
results = pd.read_sql(query, engine)

channel_names_fromsql = list(results['Channel_Name'])

# # Create a DataFrame from the list and reset the index to start from 1
df_at_sql = pd.DataFrame(channel_names_fromsql, columns=['Available channel data']).reset_index(drop=True)
df_at_sql = pd.DataFrame(channel_names_fromsql, columns=['Available channel data']).reset_index(drop=True)
# Reset index to start from 1 instead of 0
df_at_sql.index += 1
# Show dataframe
st.dataframe(df_at_sql)

# -----------------------------------------------------     /   Questions   /    ------------------------------------------------------------- #
st.subheader(':violet[Channels Analysis ]')

# Selectbox creation
question_tosql = st.selectbox('**Select your Question**',
                              ('1. What are the names of all the videos and their corresponding channels?',
                               '2. Which channels have the most number of videos, and how many videos do they have?',
                               '3. What are the top 10 most viewed videos and their respective channels?',
                               '4. How many comments were made on each video, and what are their corresponding video names?',
                               '5. Which videos have the highest number of likes, and what are their corresponding channel names?',
                               '6. What is the total number of likes and dislikes for each video, and what are their corresponding video names?',
                               '7. What is the total number of views for each channel, and what are their corresponding channel names?',
                               '8. What are the names of all the channels that have published videos in the year 2022?',
                               '9. What is the average duration of all videos in each channel, and what are their corresponding channel names?',
                               '10. Which videos have the highest number of comments, and what are their corresponding channel names?'),
                              key='collection_question')

# Creat a connection to SQL
connect_for_question = pymysql.connect(host='localhost', user='root', password='mp141534', db='youtube_harvesting')
cursor = connect_for_question.cursor()

if question_tosql == '1. What are the names of all the videos and their corresponding channels?':
    cursor.execute(
        "SELECT distinct channel.Channel_Name, video.Video_Name FROM channel JOIN playlist JOIN video ON channel.Channel_Id = playlist.Channel_Id AND playlist.Playlist_Id = video.Playlist_Id;")
    result_1 = cursor.fetchall()
    df1 = pd.DataFrame(result_1, columns=['Channel Name', 'Video Name']).reset_index(drop=True)
    df1.index += 1
    st.dataframe(df1)
elif question_tosql == '2. Which channels have the most number of videos, and how many videos do they have?':
    col1, col2 = st.columns(2)
    with col1:
        cursor.execute("SELECT distinct Channel_Name, Video_Count FROM channel ORDER BY Video_Count DESC;")
        result_2 = cursor.fetchall()
        df2 = pd.DataFrame(result_2, columns=['Channel Name', 'Video Count']).reset_index(drop=True)
        df2.index += 1
        st.dataframe(df2)
    with col2:
        fig_vc = px.bar(df2, y='Video Count', x='Channel Name', text_auto='.2s', title="Most number of videos", )
        fig_vc.update_traces(textfont_size=16, marker_color='#E6064A')
        fig_vc.update_layout(title_font_color='#1308C2 ', title_font=dict(size=25))
        st.plotly_chart(fig_vc, use_container_width=True)
elif question_tosql == '3. What are the top 10 most viewed videos and their respective channels?':
    col1, col2 = st.columns(2)
    with col1:
        cursor.execute(
            "SELECT distinct channel.Channel_Name, video.Video_Name, video.View_Count FROM channel JOIN playlist ON channel.Channel_Id = playlist.Channel_Id JOIN video ON playlist.Playlist_Id = video.Playlist_Id ORDER BY video.View_Count DESC LIMIT 10;")
        result_3 = cursor.fetchall()
        df3 = pd.DataFrame(result_3, columns=['Channel Name', 'Video Name', 'View count']).reset_index(drop=True)
        df3.index += 1
        st.dataframe(df3)
    with col2:
        fig_topvc = px.bar(df3, y='View count', x='Video Name', text_auto='.2s', title="Top 10 most viewed videos")
        fig_topvc.update_traces(textfont_size=16, marker_color='#E6064A')
        fig_topvc.update_layout(title_font_color='#1308C2 ', title_font=dict(size=25))
        st.plotly_chart(fig_topvc, use_container_width=True)
elif question_tosql == '4. How many comments were made on each video, and what are their corresponding video names?':
    cursor.execute(
        "SELECT distinct channel.Channel_Name, video.Video_Name, video.Comment_Count FROM channel JOIN playlist ON channel.Channel_Id = playlist.Channel_Id JOIN video ON playlist.Playlist_Id = video.Playlist_Id;")
    result_4 = cursor.fetchall()
    df4 = pd.DataFrame(result_4, columns=['Channel Name', 'Video Name', 'Comment count']).reset_index(drop=True)
    df4.index += 1
    st.dataframe(df4)
elif question_tosql == '5. Which videos have the highest number of likes, and what are their corresponding channel names?':
    cursor.execute(
        "SELECT distinct channel.Channel_Name, video.Video_Name, video.Like_Count FROM channel JOIN playlist ON channel.Channel_Id = playlist.Channel_Id JOIN video ON playlist.Playlist_Id = video.Playlist_Id ORDER BY video.Like_Count DESC;")
    result_5 = cursor.fetchall()
    df5 = pd.DataFrame(result_5, columns=['Channel Name', 'Video Name', 'Like count']).reset_index(drop=True)
    df5.index += 1
    st.dataframe(df5)
elif question_tosql == '6. What is the total number of likes and dislikes for each video, and what are their corresponding video names?':
    st.write('**Note:- In November 2021, YouTube removed the public dislike count from all of its videos.**')
    cursor.execute(
        "SELECT distinct channel.Channel_Name, video.Video_Name, video.Like_Count, video.Dislike_Count FROM channel JOIN playlist ON channel.Channel_Id = playlist.Channel_Id JOIN video ON playlist.Playlist_Id = video.Playlist_Id ORDER BY video.Like_Count DESC;")
    result_6 = cursor.fetchall()
    df6 = pd.DataFrame(result_6, columns=['Channel Name', 'Video Name', 'Like count', 'Dislike count']).reset_index(
        drop=True)
    df6.index += 1
    st.dataframe(df6)

elif question_tosql == '7. What is the total number of views for each channel, and what are their corresponding channel names?':
    col1, col2 = st.columns(2)
    with col1:
        cursor.execute("SELECT distinct Channel_Name, Channel_Views FROM channel ORDER BY Channel_Views DESC;")
        result_7 = cursor.fetchall()
        df7 = pd.DataFrame(result_7, columns=['Channel Name', 'Total number of views']).reset_index(drop=True)
        df7.index += 1
        st.dataframe(df7)

    with col2:
        fig_topview = px.bar(df7, y='Total number of views', x='Channel Name', text_auto='.2s',
                             title="Total number of views", )
        fig_topview.update_traces(textfont_size=16, marker_color='#E6064A')
        fig_topview.update_layout(title_font_color='#1308C2 ', title_font=dict(size=25))
        st.plotly_chart(fig_topview, use_container_width=True)

elif question_tosql == '8. What are the names of all the channels that have published videos in the year 2022?':
    cursor.execute(
        "SELECT distinct channel.Channel_Name, video.Video_Name, video.Published_date FROM channel JOIN playlist ON channel.Channel_Id = playlist.Channel_Id JOIN video ON playlist.Playlist_Id = video.Playlist_Id  WHERE EXTRACT(YEAR FROM Published_date) = 2022;")
    result_8 = cursor.fetchall()
    df8 = pd.DataFrame(result_8, columns=['Channel Name', 'Video Name', 'Year 2022 only']).reset_index(drop=True)
    df8.index += 1
    st.dataframe(df8)

elif question_tosql == '9. What is the average duration of all videos in each channel, and what are their corresponding channel names?':
    cursor.execute(
        "SELECT distinct channel.Channel_Name, TIME_FORMAT(SEC_TO_TIME(AVG(TIME_TO_SEC(TIME(video.Duration)))), '%H:%i:%s') AS duration  FROM channel JOIN playlist ON channel.Channel_Id = playlist.Channel_Id JOIN video ON playlist.Playlist_Id = video.Playlist_Id GROUP by Channel_Name ORDER BY duration DESC ;")
    result_9 = cursor.fetchall()
    df9 = pd.DataFrame(result_9, columns=['Channel Name', 'Average duration of videos (HH:MM:SS)']).reset_index(
        drop=True)
    df9.index += 1
    st.dataframe(df9)

elif question_tosql == '10. Which videos have the highest number of comments, and what are their corresponding channel names?':
    cursor.execute(
        "SELECT distinct channel.Channel_Name, video.Video_Name, video.Comment_Count FROM channel JOIN playlist ON channel.Channel_Id = playlist.Channel_Id JOIN video ON playlist.Playlist_Id = video.Playlist_Id ORDER BY video.Comment_Count DESC;")
    result_10 = cursor.fetchall()
    df10 = pd.DataFrame(result_10, columns=['Channel Name', 'Video Name', 'Number of comments']).reset_index(drop=True)
    df10.index += 1
    st.dataframe(df10)


connect_for_question.close()


