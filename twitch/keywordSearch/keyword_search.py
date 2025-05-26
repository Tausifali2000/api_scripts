import requests
import csv

# Define the API endpoint and parameters
root = "https://ensembledata.com/apis"
endpoint = "/twitch/search"
params = {
    "keyword": "magic", 
    "depth": 1,
    "type": "videos",
    "token": "SfFWgfc5TFLgQmWy"  # Replace with your actual token
}

def fetch_twitch_videos():
    res = requests.get(root + endpoint, params=params)
    if res.status_code != 200:
        print(f"Error fetching data: {res.status_code}")
        return None
    return res.json()

def format_data(data):
    videos_info = []
    videos = data.get("data", {}).get("videos", [])
    
    for video in videos:
        video_data = video.get("item", {})
        video_info = {
            "video_id": video_data.get("id", ""),
            "title": video_data.get("title", ""),
            "thumbnail": video_data.get("previewThumbnailURL", ""),
            "view_count": video_data.get("viewCount", 0),
            "created_at": video_data.get("createdAt", ""),
            "game": video_data.get("game", {}).get("displayName", ""),
            "owner": video_data.get("owner", {}).get("displayName", "")
        }
        videos_info.append(video_info)

    return videos_info

def save_to_csv(videos_info, filename):
    if not videos_info:
        print("No data to save.")
        return

    headers = ["video_id", "title", "thumbnail", "view_count", "created_at", "game", "owner"]
    with open(filename, "w", newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=headers)
        writer.writeheader()
        for video in videos_info:
            writer.writerow(video)

def main():
    response_data = fetch_twitch_videos()
    if response_data:
        formatted_data = format_data(response_data)
        save_to_csv(formatted_data, "keyword_search.csv")
        print(f"Saved {len(formatted_data)} videos to keyword_search.csv")

if __name__ == "__main__":
    main()
