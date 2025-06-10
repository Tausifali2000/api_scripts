import requests
import os
import pandas as pd
import csv
from dotenv import load_dotenv 
from datetime import datetime, timedelta, timezone
import re

load_dotenv()

TOKEN = os.getenv("ENSEMBLE_DATA_TOKEN")

csv_path = "youtube_keywordSearch_input.csv"

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

def req_youtube_hashtagSearch(keyword, depth):
    root = "https://ensembledata.com/apis"
    endpoint = "/youtube/search"
    params = {
        "keyword": keyword,
        "depth": depth,
        "start_cursor": "",
        "period": "overall",
        "sorting": "relevance",
        "get_additional_info": False,
        "token": TOKEN
    }
    res = requests.get(root + endpoint, params=params)
    return res
def format_hashtagSearch(res, keyword):
    try:
        data = res.json().get("data", {})
        posts = data.get("posts", [])
    except Exception as e:
        print(f"[ERROR] Failed to parse JSON for keyword '{keyword}': {e}")
        return []

    if not posts or not isinstance(posts, list):
        print(f"[WARNING] No valid posts found for keyword '{keyword}'")
        return []

    formatted_posts = []

    for post in posts:
        video = post.get("videoRenderer", {})
        if not video:
            continue

        try:
            post_id = video.get("videoId", "")
            post_title = (
                video.get("title", {}).get("runs", [{}])[0].get("text", "")
                if video.get("title")
                else ""
            )
            post_time_str = video.get("publishedTimeText", {}).get("simpleText", "")
            post_time = convert_relative_time_to_epoch(post_time_str)
            owner_info = video.get("ownerText", {}).get("runs", [{}])[0]
            profile_name = owner_info.get("text", "")
            profile_id = (
                    owner_info.get("navigationEndpoint", {})
                           .get("browseEndpoint", {})
                          .get("browseId", "")
                      )
            reaction_count = (
                video.get("viewCountText", {}).get("simpleText", "")
                .replace(" views", "")
                .strip()
            )
            

            row = {
                "post_id": post_id,
                "post_url": f"https://www.youtube.com/watch?v={post_id}",
                "post_text": post_title,
                "post_time": post_time,
                "profile_id": profile_id,
                "profile_name": profile_name,
                "profile_url": f"https://www.youtube.com/channel/{profile_id}" if profile_id else "",
                "reaction_count": reaction_count,
               
            }

            if post_id and post_title:
                formatted_posts.append(row)
            else:
                print(f"[INFO] Skipped incomplete post for keyword '{keyword}'")

        except Exception as e:
            print(f"[ERROR] Failed to extract fields from post for keyword '{keyword}': {e}")
            continue

    return formatted_posts

def download_keyword_search_results(user_dict):
    output_dir = "youtube_keywordSearch_output"
    output_file = os.path.join(output_dir, "youtube_keywordSearch_output.csv")

    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    all_results = []

    for row in user_dict:
        keyword = row["keyword"]
        depth = int(row.get("depth", 1))  

        print(f"Downloading keyword search results for keyword: '{keyword}'")

        res = req_youtube_hashtagSearch(keyword=keyword, depth=depth)

        if res.status_code != 200:
            print(f"[ERROR] Failed to fetch data for keyword '{keyword}' (Status code: {res.status_code})")
            continue

        formatted_data = format_hashtagSearch(res, keyword)

        if not formatted_data:
            print(f"[INFO] No data found for keyword '{keyword}'")
            continue

        all_results.extend(formatted_data)

    if all_results:
        df = pd.DataFrame(all_results)

        if os.path.exists(output_file):
            df.to_csv(output_file, mode='a', index=False, encoding="utf-8", header=False)
            print(f"[APPEND] Appended results to '{output_file}'")
        else:
            df.to_csv(output_file, index=False, encoding="utf-8")
            print(f"[CREATE] Created file and saved results to '{output_file}'")
    else:
        print("[INFO] No results to write.")



user_dict = read_csv(csv_path)
download_keyword_search_results(user_dict)
