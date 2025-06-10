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
        
        # Handle both reelItemRenderer and videoRenderer
        if "reelItemRenderer" in content:
            renderer = content["reelItemRenderer"]
            video_id = renderer.get("videoId", "")
            title = renderer.get("headline", {}).get("simpleText", "")
            view_count = renderer.get("viewCountText", {}).get("simpleText", "")
            thumbnail_data = renderer.get("thumbnail", {}).get("thumbnails", [])
            
            # Get navigation endpoint for video URL
            nav_endpoint = renderer.get("navigationEndpoint", {})
            video_url_path = nav_endpoint.get("commandMetadata", {}).get("webCommandMetadata", {}).get("url", "")
            
        elif "videoRenderer" in content:
            renderer = content["videoRenderer"]
            video_id = renderer.get("videoId", "")
            title_data = renderer.get("title", {}).get("runs", [{}])
            title = title_data[0].get("text", "") if title_data else ""
            view_count = renderer.get("viewCountText", {}).get("simpleText", "")
            thumbnail_data = renderer.get("thumbnail", {}).get("thumbnails", [])
            
            # For videoRenderer, construct URL from video ID
            video_url_path = f"/watch?v={video_id}" if video_id else ""
            
        else:
            continue  # Skip if neither renderer type
        
        # Extract thumbnail URL
        thumbnail_url = thumbnail_data[0].get("url", "") if thumbnail_data else ""
        
        # Create full video URL
        video_url = f"https://www.youtube.com{video_url_path}" if video_url_path else ""
        
        # Format according to Post data structure
        formatted_video = {
            "post_id": video_id,
            "post_url": video_url,
            "post_text": title,
            "reaction_count": view_count,  # Using view count as reaction count
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
        "reaction_count", 
    ]
    
    with open(filename, "w", newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(headers)
        for video_data in videos_data:
            writer.writerow([
                video_data["post_id"],
                video_data["post_url"],
                video_data["post_text"],
                video_data["reaction_count"],
            ])

def main():
    token = "kijGpadCYi0lZd8M"  # Replace with your actual token
    input_csv = "input.csv"
    
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
            print(f"  Views: {video['reaction_count']}")

if __name__ == "__main__":
    main()