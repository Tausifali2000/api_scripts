import requests
import csv
import os
import pandas as pd

# Define the API endpoint
root = "https://ensembledata.com/apis"
endpoint = "/youtube/search"

def read_keywords_from_csv(csv_path):
    df = pd.read_csv(csv_path, dtype=str)
    return df.to_dict(orient="records")  # Return all rows as dictionaries

def fetch_youtube_videos(keyword, depth, period, sorting, token, cursor=""):
    params = {
        "keyword": keyword,
        "depth": depth,
        "start_cursor": cursor,
        "period": period,
        "sorting": sorting,
        "get_additional_info": False,
        "token": token
    }
    
    print(f"Making request with params: {params}")
    res = requests.get(root + endpoint, params=params)
    
    if res.status_code != 200:
        print(f"Error fetching videos for keyword '{keyword}': {res.status_code}")
        print(f"Response content: {res.text}")
        print(f"Response headers: {dict(res.headers)}")
        return None
    
    try:
        return res.json()
    except requests.exceptions.JSONDecodeError as e:
        print(f"Failed to decode JSON response: {e}")
        print(f"Response content: {res.text}")
        return None

def format_video_data(data):
    video_info_list = []
    
    # Debug: Print the structure of the response
    print(f"API Response keys: {list(data.keys()) if data else 'No data'}")
    if data and "data" in data:
        print(f"Data keys: {list(data['data'].keys())}")
        if "posts" in data["data"]:
            print(f"Number of posts: {len(data['data']['posts'])}")
            if data["data"]["posts"]:
                print(f"First post keys: {list(data['data']['posts'][0].keys())}")
    
    for i, post in enumerate(data.get("data", {}).get("posts", [])):
        print(f"\nProcessing post {i+1}")
        print(f"Post keys: {list(post.keys())}")
        
        video_renderer = post.get("videoRenderer", {})
        if not video_renderer:
            print(f"No videoRenderer found in post {i+1}")
            continue
        
        print(f"VideoRenderer keys: {list(video_renderer.keys())}")
        
        # Extract video ID
        video_id = video_renderer.get("videoId", "")
        print(f"Video ID: {video_id}")
        
        # Extract title information
        title_data = video_renderer.get("title", {})
        title_text = ""
        if "runs" in title_data and title_data["runs"]:
            title_text = title_data["runs"][0].get("text", "")
        print(f"Title: {title_text}")
        
        # Extract channel information
        channel_info = video_renderer.get("longBylineText", {})
        channel_name = ""
        channel_url = ""
        if "runs" in channel_info and channel_info["runs"]:
            channel_name = channel_info["runs"][0].get("text", "")
            nav_endpoint = channel_info["runs"][0].get("navigationEndpoint", {})
            if "browseEndpoint" in nav_endpoint:
                channel_base_url = nav_endpoint["browseEndpoint"].get("canonicalBaseUrl", "")
                channel_url = f"https://www.youtube.com{channel_base_url}" if channel_base_url else ""
        
        print(f"Channel: {channel_name}")
        print(f"Channel URL: {channel_url}")
        
        # Create YouTube video URL from video ID
        video_url = f"https://www.youtube.com/watch?v={video_id}" if video_id else ""
        
        # Extract published time
        published_time = video_renderer.get("publishedTimeText", {}).get("simpleText", "")
        print(f"Published: {published_time}")
        
        # Extract view count
        view_count_text = video_renderer.get("viewCountText", {}).get("simpleText", "")
        print(f"Views: {view_count_text}")
        
        video_info = {
            "post_id": video_id,
            "post_url": video_url,
            "post_text": title_text,
            "post_time": published_time,
            "profile_name": channel_name,
            "profile_url": channel_url,
            "reaction_count": view_count_text,  # Using view count as reaction count
        }
        video_info_list.append(video_info)
        print(f"Added video: {video_info['post_text'][:50]}...")
    
    print(f"\nTotal videos processed: {len(video_info_list)}")
    return video_info_list

def save_videos_to_csv(video_info_list, filename):
    if not video_info_list:
        print("No video data to save.")
        return

    # Headers matching the required Post data format
    headers = [
        "post_id", 
        "post_url", 
        "post_text", 
        "post_time", 
        "profile_id", 
        "profile_name", 
        "profile_url", 
        "reaction_count", 
        "comment_count"
    ]
    
    with open(filename, "w", newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(headers)
        for video_info in video_info_list:
            writer.writerow([
                video_info["post_id"],
                video_info["post_url"],
                video_info["post_text"],
                video_info["post_time"],
                video_info["profile_id"],
                video_info["profile_name"],
                video_info["profile_url"],
                video_info["reaction_count"],
                video_info["comment_count"]
            ])

def main():
    input_csv = "input.csv" 
    token = "kijGpadCYi0lZd8M"  # Replace with your actual token
    output_folder = "output_data"
    os.makedirs(output_folder, exist_ok=True) 
    output_file = os.path.join(output_folder, "youtube_videos.csv")

    # Check if input file exists
    if not os.path.exists(input_csv):
        print(f"Error: Input file '{input_csv}' not found!")
        return

    try:
        keywords = read_keywords_from_csv(input_csv)
        print(f"Loaded {len(keywords)} keyword rows from CSV")
        print(f"Keywords data: {keywords}")
    except Exception as e:
        print(f"Error reading CSV file: {e}")
        return
    
    video_info_list = []
    
    for i, row in enumerate(keywords):
        print(f"\nProcessing row {i+1}: {row}")
        
        keyword = row.get("keyword", "")
        if not keyword:
            print(f"Warning: Empty keyword in row {i+1}, skipping...")
            continue
            
        # Set default values and convert types
        try:
            depth = int(row.get("depth", 1))
        except (ValueError, TypeError):
            depth = 1
            
        period = str(row.get("period", "overall"))
        sorting = str(row.get("sorting", "relevance"))
        cursor = str(row.get("cursor", ""))
        
        print(f"Using parameters - keyword: '{keyword}', depth: {depth}, period: '{period}', sorting: '{sorting}'")
        
        response_data = fetch_youtube_videos(keyword, depth, period, sorting, token, cursor)
        
        if response_data:
            print(f"API response received successfully")
            formatted_data = format_video_data(response_data)
            video_info_list.extend(formatted_data)
            print(f"Found {len(formatted_data)} videos for keyword: {keyword}")
        else:
            print(f"No data returned for keyword: {keyword}")
            print("This might be due to:")
            print("1. Invalid API token")
            print("2. Rate limiting")
            print("3. API endpoint issues")
            print("4. Invalid parameters")

    if video_info_list:
        save_videos_to_csv(video_info_list, output_file)
        print(f"Saved {len(video_info_list)} YouTube videos to {output_file}")
    else:
        print("No video data collected from any keywords")

if __name__ == "__main__":
    main()