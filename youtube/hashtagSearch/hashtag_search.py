import requests
import csv
import os
import pandas as pd

# Define the API endpoint
root = "https://ensembledata.com/apis"

def read_csv(csv_path):
    try:
        df = pd.read_csv(csv_path, dtype=str)
        if 'name' not in df.columns or 'depth' not in df.columns:
            print(f"Error: CSV file {csv_path} must have 'name' and 'depth' columns.")
            return []
        return df.to_dict(orient="records")  # Return all rows as dictionaries
    except FileNotFoundError:
        print(f"Error: CSV file {csv_path} not found.")
        return []
    except Exception as e:
        print(f"Error reading CSV: {e}")
        return []

def fetch_videos(hashtag, depth, token):
    params = {
        "name": hashtag,
        "depth": depth,
        "only_shorts": False,
        "token": token
    }
    url = root + "/youtube/hashtag/search"  # Corrected to include endpoint
    print(f"Request URL: {url}")
    response = requests.get(url, params=params)
   
    if response.status_code == 200:
        return response.json()
    else:
        print(f"Error fetching videos for #{hashtag}: {response.status_code} - {response.text}")
        return None

def format_video_data(data):
    formatted_videos = []
    videos = data.get("data", {}).get("videos", [])
    for video in videos:
        if isinstance(video, dict) and "richItemRenderer" in video:
            content = video["richItemRenderer"].get("content", {})
            if "reelItemRenderer" in content:
                renderer = content["reelItemRenderer"]
                video_id = renderer.get("videoId", "")
                title = renderer.get("headline", {}).get("simpleText", "")
                view_count = renderer.get("viewCountText", {}).get("simpleText", "")
                thumbnail = renderer.get("thumbnail", {}).get("thumbnails", [{}])[0].get("url", "")
            elif "videoRenderer" in content:
                renderer = content["videoRenderer"]
                video_id = renderer.get("videoId", "")
                title = renderer.get("title", {}).get("runs", [{}])[0].get("text", "")
                view_count = renderer.get("viewCountText", {}).get("simpleText", "")
                thumbnail = renderer.get("thumbnail", {}).get("thumbnails", [{}])[0].get("url", "")
            else:
                continue  # Skip if neither
            formatted_video = {
                "video_id": video_id,
                "title": title,
                "view_count": view_count,
                "thumbnail_url": thumbnail,
            }
            formatted_videos.append(formatted_video)
    return formatted_videos

def save_to_csv(videos_data, filename):
    if not videos_data:
        print("No videos to save.")
        return

    keys = videos_data[0].keys()
    with open(filename, "w", newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=keys)
        writer.writeheader()
        writer.writerows(videos_data)

def main():
    token = "jsK2yBd12gZlW1PI"  # Replace with your actual token
    hashtags = read_csv("input.csv")
    all_videos = []
    for row in hashtags:
        hashtag = row["name"]
        depth = row["depth"]
        print(f"Fetching videos for #{hashtag} with depth {depth}")
        res = fetch_videos(hashtag, depth, token)
        if res:
            formatted_videos = format_video_data(res)
            all_videos.extend(formatted_videos)
    
    os.makedirs("output_data", exist_ok=True)
    save_to_csv(all_videos, "output_data/youtube_hashtag_output.csv")
    print(f"Saved {len(all_videos)} videos to output_data/youtube_hashtag_output.csv")

if __name__ == "__main__":
    main()
