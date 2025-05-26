import requests
import csv

# Define the API endpoint and parameters
root = "https://ensembledata.com/apis"
endpoint = "/youtube/channel/videos"
params = {
    "browseId": "UCnQghMm3Z164JFhScQYFTBw", 
    "depth": 1,
    "token": "SfFWgfc5TFLgQmWy"  # Replace with your actual token
}

def fetch_channel_videos():
    res = requests.get(root + endpoint, params=params)
    if res.status_code != 200:
        print(f"Error fetching data: {res.status_code}")
        return None
    return res.json()

def format_data(data):
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

def save_to_csv(videos_info, filename):
    if not videos_info:
        print("No data to save.")
        return

    headers = ["video_id", "title", "thumbnail", "description", "published_time", "length", "view_count"]
    with open(filename, "w", newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(headers)
        for video in videos_info:
            writer.writerow([video["video_id"], video["title"], video["thumbnail"], video["description"], video["published_time"], video["length"], video["view_count"]])

def main():
    response_data = fetch_channel_videos()
    if response_data:
        formatted_data = format_data(response_data)
        save_to_csv(formatted_data, "channel_videos.csv")
        print(f"Saved channel videos to channel_videos.csv")

if __name__ == "__main__":
    main()
