import requests
import csv
import os
import pandas as pd
from datetime import datetime, timedelta, timezone
import re

# Define the API endpoint
root = "https://ensembledata.com/apis"

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
        'month': 'days',   
        'months': 'days',
        'year': 'days',    
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
    url = root + "/youtube/hashtag/search"
    print(f"Request URL: {url}")
    print(f"Fetching videos for #{hashtag} with depth {depth}")
    
    response = requests.get(url, params=params)
   
    if response.status_code == 200:
        print(f"Successfully fetched data for #{hashtag}")
        return response.json()
    else:
        print(f"Error fetching videos for #{hashtag}: {response.status_code} - {response.text}")
        return None

def format_video_data(data, hashtag):
    formatted_videos = []
    videos = data.get("data", {}).get("videos", [])
    
    print(f"Processing {len(videos)} videos from hashtag #{hashtag}")
    
    for i, video in enumerate(videos):
        if not isinstance(video, dict) or "richItemRenderer" not in video:
            continue
            
        content = video["richItemRenderer"].get("content", {})

        profile_id = ""
        profile_name = ""
        post_time = ""
        
        # Handle both reelItemRenderer and videoRenderer
        if "reelItemRenderer" in content:
            renderer = content["reelItemRenderer"]
            video_id = renderer.get("videoId", "")
            title = renderer.get("headline", {}).get("simpleText", "")
            view_count = renderer.get("viewCountText", {}).get("simpleText", "")
            views_count = view_count.replace("views", "").replace("view", "").replace(",", "").strip()

            
            # Get navigation endpoint for video URL
            nav_endpoint = renderer.get("navigationEndpoint", {})
            video_url_path = nav_endpoint.get("commandMetadata", {}).get("webCommandMetadata", {}).get("url", "")
            
        elif "videoRenderer" in content:
            renderer = content["videoRenderer"]
            video_id = renderer.get("videoId", "")
            title_data = renderer.get("title", {}).get("runs", [{}])
            title = title_data[0].get("text", "") if title_data else ""
            view_count = renderer.get("viewCountText", {}).get("simpleText", "")
            views_count = view_count.replace("views", "").replace("view", "").replace(",", "").strip()
            post_time = renderer.get("publishedTimeText", {}).get("simpleText", "")

        runs = renderer.get("ownerText", {}).get("runs", [])
        if runs:
                profile_id = runs[0].get("navigationEndpoint", {}).get("browseEndpoint", {}).get("browseId", "")
                profile_name = runs[0].get("text", "")

                video_url_path = f"/watch?v={video_id}" if video_id else ""
            
            
           
            
        else:
            continue  # Skip if neither renderer type
        
        
        
        # Create full video URL
        video_url = f"https://www.youtube.com{video_url_path}" if video_url_path else ""
        
        # Format according to Post data structure
        formatted_video = {
            "post_id": video_id,
            "post_url": video_url,
            "post_text": title,
            "post_time": convert_relative_time_to_epoch(post_time),
            "profile_id": profile_id,
            "profile_name": profile_name,
            "profile_url": f"https://www.youtube.com/channel/{profile_id}" if profile_id else "",
            "post_views": views_count,
        }
        formatted_videos.append(formatted_video)
    
    print(f"Successfully formatted {len(formatted_videos)} videos from hashtag #{hashtag}")
    return formatted_videos

def save_to_csv(videos_data, filename):
    if not videos_data:
        print("No videos to save.")
        return

    # Headers matching the required Post data format
    headers = [
        "post_id", 
        "post_url", 
        "post_text",  
        "post_views", 
        "post_time",
        "profile_id",
        "profile_name",
        "profile_url",
    ]
    
    with open(filename, "w", newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(headers)
        for video_data in videos_data:
            writer.writerow([
                video_data["post_id"],
                video_data["post_url"],
                video_data["post_text"],
                video_data["post_views"],
                video_data["post_time"],
                video_data["profile_id"],
                video_data["profile_name"],
                video_data["profile_url"],
            ])

def main():
    token = "kijGpadCYi0lZd8M"  # Replace with your actual token
    input_csv = "youtube_hashtagSearch_input.csv"
    
    # Check if input file exists
    if not os.path.exists(input_csv):
        print(f"Error: Input file '{input_csv}' not found!")
        print("Please create input.csv with columns: name, depth")
        print("Example:")
        print("name,depth")
        print("magic,1")
        return
    
    hashtags = read_csv(input_csv)
    if not hashtags:
        print("No hashtags to process.")
        return
        
    all_videos = []
    
    for row in hashtags:
        hashtag = row["name"]
        depth = int(row.get("depth", 1))
        
        res = fetch_videos(hashtag, depth, token)
        if res:
            formatted_videos = format_video_data(res, hashtag)
            all_videos.extend(formatted_videos)
        else:
            print(f"Failed to fetch data for hashtag: #{hashtag}")
    
    # Create output directory
    os.makedirs("output_data", exist_ok=True)
    output_file = "output_data/youtube_hashtag_output.csv"
    
    save_to_csv(all_videos, output_file)
    print(f"Saved {len(all_videos)} videos to {output_file}")
    
    if all_videos:
        print("\nSample of saved data:")
        for i, video in enumerate(all_videos[:3]):  # Show first 3 videos
            print(f"Video {i+1}: {video['post_text'][:50]}...")
            print(f"  URL: {video['post_url']}")
            print(f"  Views: {video['post_views']}")

if __name__ == "__main__":
    main()