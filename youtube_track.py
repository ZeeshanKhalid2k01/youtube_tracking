

import os
import re
import pytz
import sqlite3
import logging
import time
from dotenv import load_dotenv
from datetime import datetime, timedelta, timezone
from collections import Counter
from googleapiclient.discovery import build
from youtube_transcript_api import YouTubeTranscriptApi
from googletrans import Translator
from afinn import Afinn


# 1---Not currently working
# Setup logging
logging.basicConfig(filename='youtube_transcripts.log', 
                    level=logging.INFO,
                    format='%(asctime)s - %(message)s',
                    datefmt='%Y-%m-%d %H:%M:%S')
# 1----end

# Load API key from .env file
load_dotenv()
API_KEY = os.getenv('API_KEY')


# Set the time to PAKISTANI TIME
TIMEZONE = pytz.timezone('Asia/Karachi')

# Set up the name of sql db
DB_NAME = 'yt_transcripts.db'


# DB connection required
def get_db_connection():
    return sqlite3.connect(DB_NAME)

# Initialise the database if not created
def initialize_database():
    conn = get_db_connection()
    cursor = conn.cursor()
    
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS "All" (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            news_channel TEXT,
            day TEXT,
            date TEXT,
            time TEXT,
            transcription TEXT,
            video_title TEXT,
            video_link TEXT UNIQUE,
            video_duration TEXT
        )
    ''')

    cursor.execute('''
        CREATE TABLE IF NOT EXISTS sentimental_analysis (
            analysis_id INTEGER PRIMARY KEY AUTOINCREMENT,
            transcript_id INTEGER,
            sentence TEXT,
            sentiment REAL,
            time REAL,
            FOREIGN KEY (transcript_id) REFERENCES "All" (id)
        )
    ''')

    cursor.execute('''
        CREATE TABLE IF NOT EXISTS keywords (
            keyword_id INTEGER PRIMARY KEY AUTOINCREMENT,
            transcript_id INTEGER,
            keyword TEXT,
            intensity INTEGER,
            FOREIGN KEY (transcript_id) REFERENCES "All" (id)
        )
    ''')

    cursor.execute('''
        CREATE TABLE IF NOT EXISTS last_processed (
            channel_name TEXT PRIMARY KEY,
            last_timestamp INTEGER
        )
    ''')

    conn.commit()
    conn.close()

# load the channels from the file and return the dictionary
def load_channels(file_path):
    channels = {}
    with open(file_path, 'r') as file:
        for line in file:
            name, channel_id = line.strip().split(',')
            channels[name] = channel_id
    return channels

def get_video_details(video_id, youtube):
    response = youtube.videos().list(
        part='contentDetails,snippet',
        id=video_id
    ).execute()

    if response['items']:
        print("response", response)
        item = response['items'][0]
        content_details = item.get('contentDetails', {})
        duration = content_details.get('duration')
        if duration:
            duration = duration.replace('PT', '').replace('H', ':').replace('M', ':').replace('S', '')
        else:
            # Handle missing duration
            logging.warning(f"No duration available for video ID {video_id}")
            duration = "Unknown"
        link = f"https://www.youtube.com/watch?v={video_id}"
        return duration, link
    else:
        logging.warning(f"No video details found for video ID {video_id}")
        return None, None

# Need to check what it does and how it works
# def get_video_details(video_id, youtube):
#     response = youtube.videos().list(
#         part='contentDetails,snippet',
#         id=video_id
#     ).execute()

#     if response['items']:
#         print("response", response)
#         duration = response['items'][0]['contentDetails']['duration']
#         duration = duration.replace('PT', '').replace('H', ':').replace('M', ':').replace('S', '')
#         link = f"https://www.youtube.com/watch?v={video_id}"
#         return duration, link
#     return None, None

# Need to check what it does and how it works and why MAX_RESULTS is 100
# def get_latest_videos(channel_id, api_key, start_time, end_time, max_results=100):
#     youtube = build('youtube', 'v3', developerKey=api_key)
#     response = youtube.channels().list(part='contentDetails', id=channel_id).execute()
#     uploads_playlist_id = response['items'][0]['contentDetails']['relatedPlaylists']['uploads']
#     response = youtube.playlistItems().list(part='snippet', playlistId=uploads_playlist_id, maxResults=max_results).execute()

#     video_data = []
#     for item in response['items']:
#         video_id = item['snippet']['resourceId']['videoId']
#         video_title = item['snippet']['title']
#         published_at = datetime.strptime(item['snippet']['publishedAt'], '%Y-%m-%dT%H:%M:%SZ').replace(tzinfo=timezone.utc)

#         if start_time < published_at <= end_time:
#             duration, link = get_video_details(video_id, youtube)
#             video_data.append({
#                 'video_id': video_id,
#                 'video_title': video_title,
#                 'duration': duration,
#                 'link': link
#             })

#     return video_data

def get_latest_videos(channel_id, api_key, start_time, end_time, max_results=100):
    youtube = build('youtube', 'v3', developerKey=api_key)
    response = youtube.channels().list(part='contentDetails', id=channel_id).execute()
    uploads_playlist_id = response['items'][0]['contentDetails']['relatedPlaylists']['uploads']
    response = youtube.playlistItems().list(part='snippet', playlistId=uploads_playlist_id, maxResults=max_results).execute()

    video_data = []
    for item in response['items']:
        video_id = item['snippet']['resourceId']['videoId']
        video_title = item['snippet']['title']
        published_at = datetime.strptime(item['snippet']['publishedAt'], '%Y-%m-%dT%H:%M:%SZ').replace(tzinfo=timezone.utc)

        if start_time < published_at <= end_time:
            duration, link = get_video_details(video_id, youtube)
            if duration and duration != "Unknown":
                video_data.append({
                    'video_id': video_id,
                    'video_title': video_title,
                    'duration': duration,
                    'link': link
                })
            else:
                logging.info(f"Skipping video {video_title} ({video_id}) due to missing or unknown duration.")
    return video_data


# Extracting keywords from the transcription, How does it work, will it just provide unique words or what?
def extract_keywords(transcription):
    words = re.findall(r'\b\w+\b', transcription.lower())
    return Counter(words)

# Saving the keywords in the database
def save_keywords(transcript_id, keyword_counts):
    conn = get_db_connection()
    cursor = conn.cursor()
    
    for keyword, intensity in keyword_counts.items():
        cursor.execute('''
            INSERT INTO keywords (transcript_id, keyword, intensity)
            VALUES (?, ?, ?)
        ''', (transcript_id, keyword, intensity))
    
    conn.commit()
    conn.close()

# Divide the functions into 2 functions, one for translation and one for saving the transcript
# We need to find how translate function works, how can we detect automatically the language of the video PRIORITY:5
# def translate_and_save_transcript(video_id, translator, video_title, channel_name, video_duration, video_link):
#     try:
#         srt = YouTubeTranscriptApi.get_transcript(video_id, languages=['hi'])
#     except Exception as e:
#         logging.error(f"Transcript not available for video ID {video_id}: {e}")
#         return

#     translated_srt = []
#     full_transcription = ""

#     for line in srt:
#         translated_text = translator.translate(line['text'], src='hi', dest='en').text
#         translated_srt.append({
#             'text': translated_text,
#             'start': line['start'],
#             'duration': line['duration']
#         })
#         full_transcription += translated_text + " "

#     current_time = datetime.now(TIMEZONE)
#     day_name = current_time.strftime('%A')
#     date = current_time.strftime('%Y-%m-%d')
#     time = current_time.strftime('%H:%M:%S')

#     conn = get_db_connection()
#     cursor = conn.cursor()

#     try:
#         cursor.execute('''
#             INSERT INTO "All" (news_channel, day, date, time, transcription, video_title, video_link, video_duration) 
#             VALUES (?, ?, ?, ?, ?, ?, ?, ?)
#         ''', (channel_name, day_name, date, time, full_transcription, video_title, video_link, video_duration))
        
#         transcript_id = cursor.lastrowid
#         afinn = Afinn()

#         for line in translated_srt:
#             sentence = line['text']
#             sentiment_score = afinn.score(sentence)
#             time_str = f"{int(line['start'] // 60):02d}:{int(line['start'] % 60):02d}"

#             cursor.execute('''
#                 INSERT INTO sentimental_analysis (transcript_id, sentence, sentiment, time)
#                 VALUES (?, ?, ?, ?)
#             ''', (transcript_id, sentence, sentiment_score, time_str))

#         keyword_counts = extract_keywords(full_transcription)
#         for keyword, intensity in keyword_counts.items():
#             cursor.execute('''
#                 INSERT INTO keywords (transcript_id, keyword, intensity)
#                 VALUES (?, ?, ?)
#             ''', (transcript_id, keyword, intensity))

#         conn.commit()
#         print(f"Processed: {video_title}")
#         logging.info(f"Processed video '{video_title}' from channel '{channel_name}'")

#     except Exception as e:
#         conn.rollback()
#         logging.error(f"Error processing video {video_id}: {e}")
#         print(f"Error processing video: {e}")
    
#     finally:
#         conn.close()

def translate_transcript_batch(video_id, translator, batch_size=50):
    try:
        srt = YouTubeTranscriptApi.get_transcript(video_id, languages=['hi'])
    except Exception as e:
        logging.error(f"Transcript not available for video ID {video_id}: {e}")
        return None, None
    print("srt", srt)
    translated_srt = []
    full_transcription = ""


    # Process the transcript in batches
    for i in range(0, len(srt), batch_size):
        batch = srt[i:i + batch_size]
        combined_text = '\n'.join([line['text'] for line in batch])
        try:
            translated_text = translator.translate(combined_text, src='hi', dest='en').text
            translated_lines = translated_text.split('\n')
        except Exception as e:
            logging.error(f"Error translating batch starting at index {i}: {e}")
            continue  # Skip the batch in case of translation failure

        for j, line in enumerate(batch):
            if j < len(translated_lines):  # Check if there are enough lines in the translated output
                translated_line = {
                    'start': line['start'],
                    'duration': line['duration'],
                    'text': translated_lines[j]
                }
                translated_srt.append(translated_line)
                full_transcription += translated_lines[j] + " "

    print("translated_srt", translated_srt)
    print("full_transcription", full_transcription)
    return translated_srt, full_transcription

# def save_transcript(channel_name, video_title, video_link, video_duration, translated_srt, full_transcription):
#     current_time = datetime.now(TIMEZONE)
#     day_name = current_time.strftime('%A')
#     date = current_time.strftime('%Y-%m-%d')
#     time = current_time.strftime('%H:%M:%S')

#     conn = get_db_connection()
#     cursor = conn.cursor()

#     try:
#         cursor.execute('''
#             INSERT INTO "All" (news_channel, day, date, time, transcription, video_title, video_link, video_duration) 
#             VALUES (?, ?, ?, ?, ?, ?, ?, ?)
#         ''', (channel_name, day_name, date, time, full_transcription, video_title, video_link, video_duration))

#         transcript_id = cursor.lastrowid
#         afinn = Afinn()

#         for line in translated_srt:
#             sentence = line['text']
#             sentiment_score = afinn.score(sentence)
#             time_str = f"{int(line['start'] // 60):02d}:{int(line['start'] % 60):02d}"

#             cursor.execute('''
#                 INSERT INTO sentimental_analysis (transcript_id, sentence, sentiment, time)
#                 VALUES (?, ?, ?, ?)
#             ''', (transcript_id, sentence, sentiment_score, time_str))

#         keyword_counts = extract_keywords(full_transcription)
#         for keyword, intensity in keyword_counts.items():
#             cursor.execute('''
#                 INSERT INTO keywords (transcript_id, keyword, intensity)
#                 VALUES (?, ?, ?)
#             ''', (transcript_id, keyword, intensity))

#         conn.commit()
#         print(f"Processed: {video_title}")
#         logging.info(f"Processed video '{video_title}' from channel '{channel_name}'")

#     except Exception as e:
#         conn.rollback()
#         logging.error(f"Error processing video: {e}")
#         print(f"Error processing video: {e}")
    
#     finally:
#         conn.close()

def save_transcript(channel_name, video_title, video_link, video_duration, translated_srt, full_transcription):
    current_time = datetime.now(TIMEZONE)
    day_name = current_time.strftime('%A')
    date = current_time.strftime('%Y-%m-%d')
    time = current_time.strftime('%H:%M:%S')

    conn = get_db_connection()
    cursor = conn.cursor()

    try:
        cursor.execute('''
            INSERT INTO "All" (news_channel, day, date, time, transcription, video_title, video_link, video_duration) 
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        ''', (channel_name, day_name, date, time, full_transcription, video_title, video_link, video_duration))

        conn.commit()
        print(f"Processed and saved: {video_title}")
        logging.info(f"Processed and saved video '{video_title}' from channel '{channel_name}'")

        transcript_id = cursor.lastrowid
        afinn = Afinn()

        for line in translated_srt:
            sentence = line['text']
            sentiment_score = afinn.score(sentence)
            time_str = f"{int(line['start'] // 60):02d}:{int(line['start'] % 60):02d}"

            cursor.execute('''
                INSERT INTO sentimental_analysis (transcript_id, sentence, sentiment, time)
                VALUES (?, ?, ?, ?)
            ''', (transcript_id, sentence, sentiment_score, time_str))

        keyword_counts = extract_keywords(full_transcription)
        for keyword, intensity in keyword_counts.items():
            cursor.execute('''
                INSERT INTO keywords (transcript_id, keyword, intensity)
                VALUES (?, ?, ?)
            ''', (transcript_id, keyword, intensity))

    except sqlite3.IntegrityError as e:
        conn.rollback()  # Roll back the current transaction
        print(f"Skipped duplicate video: {video_link}")
        logging.warning(f"Skipped inserting duplicate video {video_title} with link {video_link}: {e}")
        # No need to exit the function; it will continue with the next item.

    except Exception as e:
        conn.rollback()  # Ensure changes are not committed on error
        logging.error(f"Error processing video: {e}")
        print(f"Error processing video: {e}")

    finally:
        conn.close()


# Get the last processed time from the database
def get_last_processed_time(channel_name):
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute('SELECT last_timestamp FROM last_processed WHERE channel_name = ?', (channel_name,))
    result = cursor.fetchone()
    conn.close()
    return result[0] if result else None

# Update the last processed time in the database
def update_last_processed_time(channel_name, timestamp):
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute('''
        INSERT OR REPLACE INTO last_processed (channel_name, last_timestamp) VALUES (?, ?)
    ''', (channel_name, timestamp))
    conn.commit()
    conn.close()


# how does this code works
def process_channel(channel_name, channel_id):
    now = datetime.now(TIMEZONE)
    
    print(f"\nProcessing channel: {channel_name}")
    logging.info(f"Processing channel: {channel_name}")
    
    last_processed = get_last_processed_time(channel_name)
    if last_processed:
        last_processed_time = datetime.fromtimestamp(last_processed, tz=TIMEZONE)
        hours_since_last_run = (now - last_processed_time).total_seconds() / 3600
        print(f"Last processed: {last_processed_time}")
        print(f"Hours since last run: {hours_since_last_run:.2f}")
        
        hours = hours_since_last_run
        print(f"Processing data for the past {hours:.2f} hours")
        # how does the hours=hours work
        start_time = now - timedelta(hours=hours)
    else:
        # if no channel is processed before, then start from scratch upto 24 hours
        print("No previous run detected. Starting from scratch.")
        default_hours = 24
        hours = default_hours
        print(f"Processing data for the past {hours} hours")
        start_time = now - timedelta(hours=hours)

    start_time_utc = start_time.astimezone(pytz.UTC)
    now_utc = now.astimezone(pytz.UTC)

    latest_videos = get_latest_videos(channel_id, API_KEY, start_time_utc, now_utc)
    print(f"Found {len(latest_videos)} videos for channel {channel_name}")
    logging.info(f"Found {len(latest_videos)} videos for channel {channel_name}")
    
    translator = Translator()

    for video_data in latest_videos:
        video_id = video_data['video_id']
        video_title = video_data['video_title']
        video_duration = video_data['duration']
        video_link = video_data['link']
        print("Getting transcript for video:", video_title)
        # translate_and_save_transcript(video_id, translator, video_title, channel_name, video_duration, video_link)
        translated_srt, full_transcription = translate_transcript_batch(video_id, translator)
        # return 0

        print("Translation complete.")
        if translated_srt is not None and full_transcription is not None:
            print("translated_SRT", translated_srt)
            print("\n\n\nfull_transcription", full_transcription)
            save_transcript(channel_name, video_title, video_link, video_duration, translated_srt, full_transcription)
            print("Saving transcript for video:", video_title)
        else:
            logging.error("Translation failed, no data to save.")
            print("Translation failed, no data to save.")

        

    update_last_processed_time(channel_name, int(now.timestamp()))


def main():
    initialize_database()
    channels = load_channels('channels.txt')

    for channel_name, channel_id in channels.items():
        process_channel(channel_name, channel_id)

    print("\nAll channels processed.")

if __name__ == "__main__":
    logging.info("Script started")
    print("YouTube Transcript Processing Script Started")
    main()
    print("Script execution completed.")
