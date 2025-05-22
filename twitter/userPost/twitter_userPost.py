import requests
import os
import pandas as pd
import csv
from dotenv import load_dotenv 

load_dotenv()

TOKEN = os.getenv("ENSEMBLE_DATA_TOKEN")

csv_path = "twitter_userPost_input.csv"

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

def req_twitter_user_post(tweet_id):
    root = "https://ensembledata.com/apis"
    endpoint = "/twitter/post/info"
    params = {
        "id": tweet_id,
        "token": TOKEN
    }
    res = requests.get(root + endpoint, params=params)
    return res


def format_user_post(res, tweet_id):
    try:
        result = res.json().get("data", None)
    except Exception as e:
        print(f"[ERROR] Failed to parse JSON for tweet ID {tweet_id}: {e}")
        return []

    if not result or not isinstance(result, dict):
        print(f"[WARNING] No valid data found for tweet ID {tweet_id}")
        return []

    try:
        legacy = result.get("legacy", {})
        core = result.get("core", {})
        user_results = core.get("user_results", {})
        user_result = user_results.get("result", {})
        user_legacy = user_result.get("legacy", {})

        row = {
            "post_id": result.get("rest_id", ""),
            "post_text": legacy.get("full_text", ""),
            "post_time": legacy.get("created_at", ""),
            "profile_id": legacy.get("user_id_str", ""),
            "profile_name": user_legacy.get("name", ""),
            "reaction_count": legacy.get("favorite_count", 0),
            "comment_count": legacy.get("reply_count", 0),
            "post_url": f"https://twitter.com/i/web/status/{result.get('rest_id', '')}"
        }

        if row["post_id"] and row["post_text"]:
            return [row]
        else:
            print(f"[INFO] Skipped incomplete post for tweet ID {tweet_id}")
            return []

    except Exception as e:
        print(f"[ERROR] Failed to extract fields from data for tweet ID {tweet_id}: {e}")
        return []

def download_user_post(user_dict):
    output_dir = "twitter_user_post"
    if not os.path.exists(output_dir):
        os.mkdir(output_dir)

    for row in user_dict:
        tweet_id = row["tweet_id"]
        base_filename = f"user_post_{tweet_id}.csv"
        path = os.path.join(output_dir, base_filename)

        
        suffix = 1
        while os.path.exists(path):
            suffix += 1
            path = os.path.join(output_dir, f"user_post_{tweet_id}_{suffix}.csv")

        print(f"Downloading post for tweet ID {tweet_id}")
        res = req_twitter_user_post(tweet_id=tweet_id)

        if res.status_code != 200:
            print(f"Failed to fetch data for tweet ID {tweet_id}")
            continue

        formatted_data = format_user_post(res, tweet_id)

        if not formatted_data:
            print(f"No data found for tweet ID {tweet_id}")
            continue

        df = pd.DataFrame(formatted_data)
        df.to_csv(path, index=False, encoding="utf-8")
        print(f"Saved post to {path}")

# Run the script
user_dict = read_csv("twitter_userPost_input.csv")
download_user_post(user_dict=user_dict)
