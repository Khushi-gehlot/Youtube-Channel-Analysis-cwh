# Databricks notebook source
pip install google-api-python-client pandas


# COMMAND ----------

# DBTITLE 1,Extract
from googleapiclient.discovery import build
import pandas as pd


API_KEY = "-api-key-"
CHANNEL_ID = "-channel-id-" 

youtube = build('youtube', 'v3', developerKey=API_KEY)


channel_request = youtube.channels().list(
    part="contentDetails",
    id=CHANNEL_ID
)
channel_response = channel_request.execute()
playlist_id = channel_response['items'][0]['contentDetails']['relatedPlaylists']['uploads']

#GetING all videos from playlist
videos = []
next_page_token = None

while True:
    pl_request = youtube.playlistItems().list(
        part="snippet,contentDetails",
        playlistId=playlist_id,
        maxResults=50,
        pageToken=next_page_token
    )
    pl_response = pl_request.execute()

    for item in pl_response['items']:
        video_id = item['contentDetails']['videoId']
        title = item['snippet']['title']
        publish_date = item['contentDetails']['videoPublishedAt']

        # Fetch statistics
        vid_request = youtube.videos().list(
            part="statistics",
            id=video_id
        )
        vid_response = vid_request.execute()
        stats = vid_response['items'][0]['statistics']

        videos.append({
            'video_id': video_id,
            'title': title,
            'publish_date': publish_date,
            'views': stats.get('viewCount', 0),
            'likes': stats.get('likeCount', 0),
            'comments': stats.get('commentCount', 0)
        })

    next_page_token = pl_response.get('nextPageToken')
    if not next_page_token:
        break


df = pd.DataFrame(videos)
df.to_csv("CWH_youtube_data.csv", index=False)
print("Data saved to CWH_youtube_data.csv")


# COMMAND ----------

# DBTITLE 1,Transform
import pandas as pd

df = pd.read_csv("CWH_youtube_data.csv")

# Convert publish date to datetime
df['publish_date'] = pd.to_datetime(df['publish_date'])

# Calculate days since publish
df['publish_date'] = pd.to_datetime(df['publish_date']).dt.tz_localize(None)
df['days_since_publish'] = (pd.Timestamp.today() - df['publish_date']).dt.days


# Convert numeric columns to integers
df['views'] = pd.to_numeric(df['views'], errors='coerce').fillna(0).astype(int)
df['likes'] = pd.to_numeric(df['likes'], errors='coerce').fillna(0).astype(int)
df['comments'] = pd.to_numeric(df['comments'], errors='coerce').fillna(0).astype(int)

# Engagement rate
df['engagement_rate'] = ((df['likes'] + df['comments']) / df['views']).round(4)

# Views per day
df['views_per_day'] = (df['views'] / df['days_since_publish']).round(2)

df.to_csv("CWH_youtube_transformed.csv", index=False)



# COMMAND ----------

# DBTITLE 1,Load
import sqlite3
import pandas as pd

df = pd.read_csv("CWH_youtube_transformed.csv")
conn = sqlite3.connect("youtube_data.db")

# Load DataFrame into SQLite table
df.to_sql("CWH_videos", conn, if_exists="replace", index=False)

conn.commit()
conn.close()

print("Data loaded into SQLite database: youtube_data.db")


# COMMAND ----------

conn = sqlite3.connect("youtube_data.db")
cursor = conn.cursor()

for row in cursor.execute("SELECT title, views FROM CWH_videos ORDER BY views DESC LIMIT 5"):
    print(row)

conn.close()
