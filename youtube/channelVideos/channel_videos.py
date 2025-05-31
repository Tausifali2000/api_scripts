import requests
import csv
import os

# Define the API endpoint
root = "https://ensembledata.com/apis"
endpoint = "/youtube/channel/videos"

def read_params_from_csv(filename):
    params_list = []
    with open(filename, "r", encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            params_list.append(row)
    return params_list

def fetch_channel_videos(params):
    res = requests.get(root + endpoint, params=params)
    if res.status_code != 200:
        print(f"Error fetching data: {res.status_code}")
        return None
    return res.json()

def format_channel_data(data):
    channel_info = data.get("data", {}).get("user", {})
    channel_details = {
        "url": channel_info.get("urlCanonical", ""),
        "title": channel_info.get("title", ""),
        "description": channel_info.get("description", ""),
        "thumbnail": channel_info.get("thumbnail", {}).get("thumbnails", [{}])[0].get("url", ""),
        "tags": channel_info.get("tags", []),
    }
    return channel_details

def format_video_data(data):
    videos_info = []
    videos = data.get("data", {}).get("videos", [])
    
    for video in videos:
        video_data = video.get("richItemRenderer", {}).get("content", {}).get("videoRenderer", {})
        video_info = {
            "video_id": video_data.get("videoId", ""),
            "title": video_data.get("title", {}).get("runs", [{}])[0].get("text", ""),
            "thumbnail": video_data.get("thumbnail", {}).get("thumbnails", [{}])[0].get("url", ""),
            "description": video_data.get("descriptionSnippet", {}).get("runs", [{}])[0].get("text", ""),
            "published_time": video_data.get("publishedTimeText", {}).get("simpleText", ""),
            "length": video_data.get("lengthText", {}).get("simpleText", ""),
            "view_count": video_data.get("viewCountText", {}).get("simpleText", "")
        }
        videos_info.append(video_info)

    return videos_info

def save_to_csv(channel_info, videos_info, filename):
    if not videos_info:
        print("No video data to save.")
        return

    headers = ["channel_url", "channel_title", "channel_description", "channel_thumbnail", "tags", "video_id", "title", "thumbnail", "description", "published_time", "length", "view_count"]
    with open(filename, "w", newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(headers)
        for video in videos_info:
            writer.writerow([
                channel_info["url"],
                channel_info["title"],
                channel_info["description"],
                channel_info["thumbnail"],
                ", ".join(channel_info["tags"]),
                video["video_id"],
                video["title"],
                video["thumbnail"],
                video["description"],
                video["published_time"],
                video["length"],
                video["view_count"]
            ])

def main():
    params_list = read_params_from_csv("input.csv")
    
    output_dir = "output"
    os.makedirs(output_dir, exist_ok=True)
    
    output_filename = os.path.join(output_dir, "channel_videos_input.csv")
    
    all_videos_info = []
    channel_info = None
    
    for params in params_list:
        browse_id = params['browseId']
        request_params = {
            "browseId": browse_id,
            "depth": 1,
            "token": "B3jQjPFTs8Y88lXj"  
        }
        response_data = fetch_channel_videos(request_params)
        if response_data:
            if channel_info is None:
                channel_info = format_channel_data(response_data)  
            videos_info = format_video_data(response_data)
            all_videos_info.extend(videos_info)  

    # Save all collected data to a single CSV file
    save_to_csv(channel_info, all_videos_info, output_filename)
    print(f"Saved channel and video details to {output_filename}")

if __name__ == "__main__":
    main()
