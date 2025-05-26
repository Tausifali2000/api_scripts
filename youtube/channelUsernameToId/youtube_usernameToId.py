import requests
import os
import pandas as pd
import csv
from dotenv import load_dotenv 

load_dotenv()

TOKEN = os.getenv("ENSEMBLE_DATA_TOKEN")

csv_path = "youtube_usernameToId_input.csv"

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

def req_youtube_channel_id(username):
    root = "https://ensembledata.com/apis"
    endpoint = "/youtube/channel/name-to-id"
    params = {
        "name": username,
        "token": TOKEN
    }
    res = requests.get(root + endpoint, params=params)
    return res

def format_channel_id(res, username):
    try:
        data = res.json().get("data", None)
    except Exception as e:
        print(f"[ERROR] Failed to parse JSON for username {username}: {e}")
        return []

    if not data or not isinstance(data, str):  
        print(f"[WARNING] No valid data found for username {username}")
        return []

    try:
        row = {
            "username": username,
            "channel_id": data
        }
        return [row]
    except Exception as e:
        print(f"[ERROR] Failed to extract fields for username {username}: {e}")
        return []

def download_channel_ids(user_dict):
    output_dir = "youtube_usernameToId_output"
    if not os.path.exists(output_dir):
        os.mkdir(output_dir)

    for row in user_dict:
        username = row["username"]
        base_filename = f"channel_id_{username}.csv"
        path = os.path.join(output_dir, base_filename)

        suffix = 1
        while os.path.exists(path):
            suffix += 1
            path = os.path.join(output_dir, f"channel_id_{username}_{suffix}.csv")

        print(f"Fetching channel ID for username {username}")
        res = req_youtube_channel_id(username=username)

        if res.status_code != 200:
            print(f"[ERROR] Failed to fetch data for username {username}")
            continue

        formatted_data = format_channel_id(res, username)

        if not formatted_data:
            print(f"[INFO] No data found for username {username}")
            continue

        df = pd.DataFrame(formatted_data)
        df.to_csv(path, index=False, encoding="utf-8")
        print(f"[SUCCESS] Saved channel ID to {path}")

# Run the script
user_dict = read_csv("youtube_usernameToId_input.csv")
download_channel_ids(user_dict=user_dict)
