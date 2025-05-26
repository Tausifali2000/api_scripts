import requests
import os
import pandas as pd
import csv
from dotenv import load_dotenv 

load_dotenv()

TOKEN = os.getenv("ENSEMBLE_DATA_TOKEN")

csv_path = "youtube_subCount_input.csv"

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

def req_youtube_sub_count(channel_id):
    root = "https://ensembledata.com/apis"
    endpoint = "/youtube/channel/followers"
    params = {
        "browseId": channel_id,
        "token": TOKEN
    }
    res = requests.get(root + endpoint, params=params)
    return res

def format_sub_count(res, channel_id):
    try:
        data = res.json().get("data", None)
    except Exception as e:
        print(f"[ERROR] Failed to parse JSON for channel ID {channel_id}: {e}")
        return []

    if not data or not isinstance(data, str):  # Expecting sub count string like "12.8K"
        print(f"[WARNING] No valid data found for channel ID {channel_id}")
        return []

    try:
        row = {
            "channel_id": channel_id,
            "subscribers_count": data
        }
        return [row]
    except Exception as e:
        print(f"[ERROR] Failed to extract fields for channel ID {channel_id}: {e}")
        return []

def download_sub_counts(user_dict):
    output_dir = "youtube_subCount_output"
    if not os.path.exists(output_dir):
        os.mkdir(output_dir)

    for row in user_dict:
        channel_id = row["channel_id"]
        base_filename = f"sub_count_{channel_id}.csv"
        path = os.path.join(output_dir, base_filename)

        suffix = 1
        while os.path.exists(path):
            suffix += 1
            path = os.path.join(output_dir, f"sub_count_{channel_id}_{suffix}.csv")

        print(f"Fetching subscriber count for channel ID {channel_id}")
        res = req_youtube_sub_count(channel_id=channel_id)

        if res.status_code != 200:
            print(f"[ERROR] Failed to fetch data for channel ID {channel_id}")
            continue

        formatted_data = format_sub_count(res, channel_id)

        if not formatted_data:
            print(f"[INFO] No data found for channel ID {channel_id}")
            continue

        df = pd.DataFrame(formatted_data)
        df.to_csv(path, index=False, encoding="utf-8")
        print(f"[SUCCESS] Saved subscriber count to {path}")

# Run the script
user_dict = read_csv("youtube_subCount_input.csv")
download_sub_counts(user_dict=user_dict)
