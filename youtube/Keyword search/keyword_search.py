import requests
import csv

# Define the API endpoint and parameters
root = "https://ensembledata.com/apis"
endpoint = "/youtube/search"
params = {
    "keyword": "magic", 
    "depth": 1,
    "start_cursor": "",
    "period": "overall",
    "sorting": "relevance",
    "get_additional_info": False,
    "token": "SfFWgfc5TFLgQmWy"  # Replace with your actual token
}

def fetch_youtube_data():
    res = requests.get(root + endpoint, params=params)
    if res.status_code != 200:
        print(f"Error fetching data: {res.status_code}")
        return None
    return res.json()

def format_data(data):
    videos = []
    for post in data.get("data", {}).get("posts", []):
        video_data = post.get("videoRenderer", {})
        video_info = {
            "video_id": video_data.get("videoId", ""),
            "title": video_data.get("title", {}).get("runs", [{}])[0].get("text", ""),
            "thumbnail": video_data.get("thumbnail", {}).get("thumbnails", [{}])[0].get("url", ""),
            "published_time": video_data.get("publishedTimeText", {}).get("simpleText", ""),
            "length": video_data.get("lengthText", {}).get("simpleText", ""),
            "view_count": video_data.get("viewCountText", {}).get("simpleText", ""),
            "channel": video_data.get("longBylineText", {}).get("runs", [{}])[0].get("text", ""),
            "channel_url": f"https://www.youtube.com/channel/{video_data.get('longBylineText', {}).get('runs', [{}])[0].get('navigationEndpoint', {}).get('browseEndpoint', {}).get('browseId', '')}"

        }
        videos.append(video_info)
    return videos

def save_to_csv(videos, filename):
    if not videos:
        print("No data to save.")
        return

    headers = videos[0].keys()
    with open(filename, "w", newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=headers)
        writer.writeheader()
        for video in videos:
            writer.writerow(video)

def main():
    response_data = fetch_youtube_data()
    if response_data:
        formatted_videos = format_data(response_data)
        save_to_csv(formatted_videos, "keyword_search.csv")
        print(f"Saved {len(formatted_videos)} videos to keyword_search.csv")

if __name__ == "__main__":
    main()
