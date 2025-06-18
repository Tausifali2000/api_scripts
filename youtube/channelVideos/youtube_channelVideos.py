import requests
import csv
import os
from datetime import datetime, timedelta, timezone
import re


# Define the API endpoint
root = "https://ensembledata.com/apis"
endpoint = "/youtube/channel/videos"

def convert_relative_time_to_epoch(relative_time: str) -> int:
    relative_time = relative_time.strip().lower()
    match = re.match(
        r"(\d+)\s+(second|seconds|minute|minutes|hour|hours|day|days|week|weeks|month|months|year|years)\s+ago",
        relative_time
    )

    if not match:
        print(f"[WARN] Unrecognized format: '{relative_time}', using current time as fallback.")
        return int(datetime.now(timezone.utc).timestamp())

    value = int(match.group(1))
    unit = match.group(2)

    # Approximate conversions
    time_map = {
        "second": timedelta(seconds=value),
        "seconds": timedelta(seconds=value),
        "minute": timedelta(minutes=value),
        "minutes": timedelta(minutes=value),
        "hour": timedelta(hours=value),
        "hours": timedelta(hours=value),
        "day": timedelta(days=value),
        "days": timedelta(days=value),
        "week": timedelta(weeks=value),
        "weeks": timedelta(weeks=value),
        "month": timedelta(days=value * 30),   # Approximate
        "months": timedelta(days=value * 30),
        "year": timedelta(days=value * 365),   # Approximate
        "years": timedelta(days=value * 365),
    }

    delta = time_map.get(unit, timedelta())
    target_time = datetime.now(timezone.utc) - delta
    return int(target_time.timestamp())


def read_params_from_csv(filename):
    """Read parameters from CSV file"""
    params_list = []
    with open(filename, "r", encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            params_list.append(row)
    return params_list

def fetch_channel_videos(params):
    """Fetch channel videos data from API"""
    res = requests.get(root + endpoint, params=params)
    if res.status_code != 200:
        print(f"Error fetching data: {res.status_code}")
        return None
    return res.json()

def format_post_data(data):
    """Format video data as posts"""
    posts_info = []
    videos = data.get("data", {}).get("videos", [])
    channel_info = data.get("data", {}).get("user", {})
    
    # Extract channel details for profile information
    profile_id = channel_info.get("urlCanonical", "").split("/")[-1] if channel_info.get("urlCanonical") else ""
    profile_name = channel_info.get("title", "")
    profile_url = channel_info.get("urlCanonical", "")
    
    for video in videos:
        video_data = video.get("richItemRenderer", {}).get("content", {}).get("videoRenderer", {})

        relative_time = video_data.get("publishedTimeText", {}).get("simpleText", "")
        epoch_time = convert_relative_time_to_epoch(relative_time) if relative_time else None
        
        # Map video data to post structure
        post_info = {
            "post_id": video_data.get("videoId", ""),
            "post_url": f"https://www.youtube.com/watch?v={video_data.get('videoId', '')}",
            "post_text": video_data.get("title", {}).get("runs", [{}])[0].get("text", ""),
            "post_time": epoch_time,
            "profile_id": profile_id,
            "profile_name": profile_name,
            "profile_url": profile_url,
            "post_views": extract_numeric_value(video_data.get("viewCountText", {}).get("simpleText", "0")),
        }
        posts_info.append(post_info)
    
    return posts_info

def extract_numeric_value(text):
    """Extract numeric value from text (e.g., '1.2K views' -> 1200)"""
    if not text:
        return 0
    
    # Remove non-numeric characters except for K, M, B
    import re
    match = re.search(r'([\d,\.]+)\s*([KMB]?)', text.replace(',', ''))
    if match:
        number = float(match.group(1))
        multiplier = match.group(2).upper()
        
        if multiplier == 'K':
            return int(number * 1000)
        elif multiplier == 'M':
            return int(number * 1000000)
        elif multiplier == 'B':
            return int(number * 1000000000)
        else:
            return int(number)
    return 0

def save_posts_to_csv(posts_info, filename):
    """Save post data to CSV"""
    if not posts_info:
        print("No post data to save.")
        return
    
    headers = ["post_id", "post_url", "post_text", "post_time", "profile_id", 
               "profile_name", "profile_url", "post_views"]
    
    with open(filename, "w", newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(headers)
        for post in posts_info:
            writer.writerow([
                post["post_id"],
                post["post_url"],
                post["post_text"],
                post["post_time"],
                post["profile_id"],
                post["profile_name"],
                post["profile_url"],
                post["post_views"],
            ])

def main():
    """Main function to orchestrate the data fetching and processing"""
    params_list = read_params_from_csv("youtube_channelVideos_input.csv")
    
    # Create output directory
    output_dir = "output"
    os.makedirs(output_dir, exist_ok=True)
    
    # Initialize data collections
    all_posts_info = []
    
    for params in params_list:
        browse_id = params['browseId']
        request_params = {
            "browseId": browse_id,
            "depth": 1,
            "token": "kijGpadCYi0lZd8M"  # Your API token
        }
        
        print(f"Fetching data for browseId: {browse_id}")
        response_data = fetch_channel_videos(request_params)
        
        if response_data:
            # Process posts (videos)
            posts_info = format_post_data(response_data)
            all_posts_info.extend(posts_info)
                    
    # Save all collected data to separate CSV files
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    
    posts_filename = os.path.join(output_dir, f"channel_videos_input.csv")
    
    save_posts_to_csv(all_posts_info, posts_filename)
    
    print(f"Saved {len(all_posts_info)} posts to {posts_filename}")
    
    # Print summary
    print("\n=== Data Collection Summary ===")
    print(f"Total Posts (Videos): {len(all_posts_info)}")

if __name__ == "__main__":
    main()