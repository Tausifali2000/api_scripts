import requests
import os
import pandas as pd
import csv
from dotenv import load_dotenv 
from datetime import datetime, timedelta, timezone
import re

load_dotenv()

TOKEN = os.getenv("ENSEMBLE_DATA_TOKEN")

csv_path = "youtube_shortDetails_input.csv"

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





def read_csv(csv_path):
    df = pd.read_csv(csv_path, dtype=str)  
    rows = df.to_dict(orient="records")   
    return rows

def save_csv(dict_data, path):
    headers = dict_data[0].keys()
    with open(path, "w") as f:
        writer = csv.DictWriter(f, fieldnames=headers)
        writer.writeheader()
        writer.writerows(dict_data)

def req_youtube_short_details(short_id):
    root = "https://ensembledata.com/apis"
    endpoint = "/youtube/channel/get-short-stats"
    params = {
        "id": short_id,
        "token": TOKEN,
        
    }
    res = requests.get(root + endpoint, params=params)
    return res


def format_short_details(res, short_id):
    try:
        data = res.json().get("data", None)
    except Exception as e:
        print(f"[ERROR] Failed to parse JSON for short ID {short_id}: {e}")
        return []

    if not data or not isinstance(data, dict):
        print(f"[WARNING] No valid data found for short ID {short_id}")
        return []

    try:
        # Safe navigation with nested dicts
        like_button = data.get("likeButton", {}).get("likeButtonRenderer", {})
        header = data.get("reelPlayerHeaderSupportedRenderers", {}).get("reelPlayerHeaderRenderer", {})

        post_id = like_button.get("target", {}).get("videoId", "")
        post_title = header.get("reelTitleText", {}).get("runs", [{}])[0].get("text", "")
        post_time_str = header.get("timestampText", {}).get("simpleText", "")
        post_time = convert_relative_time_to_epoch(post_time_str)
        profile_id = header.get("channelNavigationEndpoint", {}).get("browseEndpoint", {}).get("browseId", "")
        profile_name = header.get("channelTitleText", {}).get("runs", [{}])[0].get("text", "")
        reaction_count = like_button.get("likeCount", 0)
        comment_count = data.get("viewCommentsButton", {}).get("buttonRenderer", {}).get("accessibility", {}).get("label", "")

        row = {
            "post_id": post_id,
            "post_title": post_title,
            "post_time": post_time,
            "profile_id": profile_id,
            "profile_name": profile_name,
            "reaction_count": reaction_count,
            "comment_count": comment_count,
            "post_url": f"https://www.youtube.com/shorts/{post_id}"
        }

        if post_id and post_title:
            return [row]
        else:
            print(f"[INFO] Skipped incomplete post for short ID {short_id}")
            return []

    except Exception as e:
        print(f"[ERROR] Failed to extract fields from data for short ID {short_id}: {e}")
        return []

def download_short_details(user_dict):
    output_dir = "youtube_short_details"
    if not os.path.exists(output_dir):
        os.mkdir(output_dir)

    for row in user_dict:
        short_id = row["short_id"]
        base_filename = f"short_details_{short_id}.csv"
        path = os.path.join(output_dir, base_filename)

        
        suffix = 1
        while os.path.exists(path):
            suffix += 1
            path = os.path.join(output_dir, f"short_details_{short_id}_{suffix}.csv")

        print(f"Downloading post for short ID {short_id}")
        res = req_youtube_short_details(short_id=short_id)

        if res.status_code != 200:
            print(f"Failed to fetch data for short ID {short_id}")
            continue

        formatted_data = format_short_details(res, short_id)

        if not formatted_data:
            print(f"No data found for short ID {short_id}")
            continue

        df = pd.DataFrame(formatted_data)
        df.to_csv(path, index=False, encoding="utf-8")
        print(f"Saved post to {path}")

# Run the script
user_dict = read_csv("youtube_shortDetails_input.csv")
download_short_details(user_dict=user_dict)
