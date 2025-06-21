import requests
import os
import pandas as pd
import csv
from dotenv import load_dotenv 
from datetime import datetime, timedelta, timezone
import re

load_dotenv()

TOKEN = os.getenv("ENSEMBLE_DATA_TOKEN")

csv_path = "youtube_channelStreams_input.csv"

def convert_relative_time_to_epoch(relative_time: str) -> int:
  
   
    now = datetime.now(timezone.utc)

    time_map = {
        'second': 'seconds',
        'seconds': 'seconds',
        'minute': 'minutes',
        'minutes': 'minutes',
        'hour': 'hours',
        'hours': 'hours',
        'day': 'days',
        'days': 'days',
        'week': 'weeks',
        'weeks': 'weeks',
        'month': 'days',   # Approximate months as 30 days
        'months': 'days',
        'year': 'days',    # Approximate years as 365 days
        'years': 'days'
    }

    match = re.match(r"(\d+)\s+(second|seconds|minute|minutes|hour|hours|day|days|week|weeks|month|months|year|years)\s+ago", relative_time.strip())
    if not match:
        return int(now.timestamp())

    value, unit = int(match.group(1)), match.group(2)

    if unit in ['month', 'months']:
        delta = timedelta(days=value * 30)
    elif unit in ['year', 'years']:
        delta = timedelta(days=value * 365)
    else:
        delta = timedelta(**{time_map[unit]: value})

    target_time = now - delta
    return int(target_time.timestamp())

def parse_view_count(view_str: str) -> int:
    if not view_str:
        return 0

    view_str = view_str.strip().upper().replace(",", "").replace(" VIEWS", "")

    try:
        if view_str.endswith("K"):
            return int(float(view_str[:-1]) * 1_000)
        elif view_str.endswith("M"):
            return int(float(view_str[:-1]) * 1_000_000)
        elif view_str.endswith("B"):
            return int(float(view_str[:-1]) * 1_000_000_000)
        else:
            return int(view_str)  # fallback for raw numbers like "12345"
    except:
        return 0


def read_csv(csv_path):
    df = pd.read_csv(csv_path, dtype=str)  
    rows = df.to_dict(orient="records")   
    return rows


def req_youtube_channelStreams(browseId):
    root = "https://ensembledata.com/apis"
    endpoint = "/youtube/channel/streams"
    params = {
        "browseId": browseId,
        "depth": 1,
        "token": TOKEN,
        
    }
    res = requests.get(root + endpoint, params=params)
    return res


def format_channel_streams(res):
    try:
        data = res.json().get("data", {})
        streams = data.get("streams", [])
        channel_info = data.get("user", {})

        profile_id = channel_info.get("urlCanonical", "").split("/")[-1] if channel_info.get("urlCanonical") else ""
        profile_name = channel_info.get("title", "")
        profile_url = channel_info.get("urlCanonical", "")
       

    except Exception as e:
        print(f"[ERROR] Failed to parse Shorts JSON: {e}")
        return []

    formatted_posts = []

    for item in streams:
        try:
            video = item.get("richItemRenderer", {}).get("content", {}).get("videoRenderer", {})
            post_id = video.get("videoId", "")
            if not post_id:
                continue
            
            
            post_text = video.get("title", {}).get("runs", [{}])[0].get("text", "")
            published_text = video.get("publishedTimeText", {}).get("simpleText", "")
            relative_time_clean = re.sub(r"^(Streamed|Premiered|Uploaded)\s+", "", published_text)
            
            post_time_epoch = convert_relative_time_to_epoch(relative_time_clean)

            # post_views = video.get("viewCountText", {}).get("runs", [{}])[0].get("text", "")

            row = {
                "post_id": post_id,
                "post_url": f"https://www.youtube.com/shorts/{post_id}",
                "post_text": post_text,
                "post_time":  post_time_epoch,
                "profile_id": profile_id,
                "profile_name": profile_name,
                "profile_url": profile_url,
                
                
                
            }

            formatted_posts.append(row)

        except Exception as e:
            print(f"[ERROR] Failed to parse Shorts item: {e}")
            continue

    return formatted_posts
   

def download_channelStreams_results(user_dict):
    output_dir = "youtube_channelStreams_output"
    output_file = os.path.join(output_dir, "youtube_channelStreams_output.csv")

    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    all_results = []

    for row in user_dict:
        browseId = row.get("browseId", "").strip()

        if not browseId:
            print("[WARNING] Skipping row with missing 'browseId'")
            continue

        print(f"[INFO] Fetching Streams for channel: {browseId}")

        res = req_youtube_channelStreams(browseId)

        if res.status_code != 200:
            print(f"[ERROR] Failed to fetch Streams for '{browseId}' (Status code: {res.status_code})")
            continue

        formatted_data = format_channel_streams(res)

        if not formatted_data:
            print(f"[INFO] No Streams data found for channel '{browseId}'")
            continue

        print(f"[SUCCESS] Fetched {len(formatted_data)} Streams for channel '{browseId}'")
        all_results.extend(formatted_data)

    if all_results:
        df = pd.DataFrame(all_results)

        if os.path.exists(output_file):
            df.to_csv(output_file, mode='a', index=False, encoding="utf-8", header=False)
            print(f"[APPEND] Appended {len(df)} new rows to '{output_file}'")
        else:
            df.to_csv(output_file, index=False, encoding="utf-8")
            print(f"[CREATE] Created file and saved {len(df)} rows to '{output_file}'")
    else:
        print("[INFO] No results to write.")



user_dict = read_csv(csv_path)
download_channelStreams_results(user_dict)