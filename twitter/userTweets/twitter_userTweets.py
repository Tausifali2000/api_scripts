import requests
import os
import pandas as pd
import csv
from dotenv import load_dotenv 

load_dotenv()

TOKEN = os.getenv("ENSEMBLE_DATA_TOKEN")
csv_path = "twitter_userTweets_input.csv"

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

def req_twitter_user_tweets(user_id):
    root = "https://ensembledata.com/apis"
    endpoint = "/twitter/user/tweets"
    params = {
        "id": user_id,
        "token": TOKEN
    }
    res = requests.get(root + endpoint, params=params)
    return res

def format_user_tweets(res, user_id):
    try:
        data = res.json().get("data", None)
    except Exception as e:
        print(f"[ERROR] Failed to parse JSON for user ID {user_id}: {e}")
        return []

    if not data or not isinstance(data, list):
        print(f"[WARNING] No valid data found for user ID {user_id}")
        return []

    rows = []

    for tweet_data in data:
        try:
            content = tweet_data.get("content", {})
            itemContent = content.get("itemContent", {})
            tweet_results = itemContent.get("tweet_results", {})
            result = tweet_results.get("result", {})

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
                rows.append(row)
            else:
                print(f"[INFO] Skipped incomplete tweet for user ID {user_id}")

        except Exception as e:
            print(f"[ERROR] Failed to parse tweet data for user ID {user_id}: {e}")
            continue

    if not rows:
        print(f"[INFO] No tweets formatted for user ID {user_id}")

    return rows


def download_user_tweets(user_dict):
    output_dir = "twitter_user_tweets"
    if not os.path.exists(output_dir):
        os.mkdir(output_dir)

    for row in user_dict:
        user_id = row["user_id"]
        base_filename = f"user_tweets_{user_id}.csv"
        path = os.path.join(output_dir, base_filename)

       
        suffix = 1
        while os.path.exists(path):
            suffix += 1
            path = os.path.join(output_dir, f"user_tweets_{user_id}_{suffix}.csv")

        print(f"Downloading tweets for user ID {user_id}")
        res = req_twitter_user_tweets(user_id=user_id)
        
        if res.status_code != 200:
            print(f"Failed to fetch data for {user_id}")
            continue

        formatted_data = format_user_tweets(res, user_id)

        if len(formatted_data) == 0:
            print(f"No tweets found for {user_id}")
            continue

        df = pd.DataFrame(formatted_data)
        df.to_csv(path, index=False, encoding="utf-8")
        print(f"Saved {len(formatted_data)} tweets to {path}")


user_dict = read_csv( "twitter_userTweets_input.csv")
download_user_tweets(user_dict=user_dict)
