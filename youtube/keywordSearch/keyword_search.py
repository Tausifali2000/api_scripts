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
    res = requests.get(root + endpoint, params=params)
    if res.status_code != 200:
        print(f"Error fetching videos for keyword '{keyword}': {res.status_code}")
        return None
    return res.json()

def format_video_data(data):
    video_info_list = []
    for post in data.get("data", {}).get("posts", []):
        video_renderer = post.get("videoRenderer", {})
        if not video_renderer:  # Skip if no video renderer
            continue
        channel_info = video_renderer.get("longBylineText", {}).get("runs", [{}])[0]
        video_info = {
            "video_id": video_renderer.get("videoId", ""),
            "title": video_renderer.get("title", {}).get("runs", [{}])[0].get("text", ""),
            "thumbnail_url": video_renderer.get("thumbnail", {}).get("thumbnails", [{}])[0].get("url", ""),
            "published_time": video_renderer.get("publishedTimeText", {}).get("simpleText", ""),
            "length": video_renderer.get("lengthText", {}).get("simpleText", ""),
            "view_count": video_renderer.get("viewCountText", {}).get("simpleText", ""),
            "channel_name": channel_info.get("text", ""),
            "channel_url": channel_info.get("navigationEndpoint", {}).get("browseEndpoint", {}).get("canonicalBaseUrl", "")
        }
        video_info_list.append(video_info)
    return video_info_list

def save_videos_to_csv(video_info_list, filename):
    if not video_info_list:
        print("No video data to save.")
        return

    headers = ["video_id", "title", "thumbnail_url", "published_time", "length", "view_count", "channel_name", "channel_url"]
    with open(filename, "w", newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(headers)
        for video_info in video_info_list:
            # Only write if video_info has valid data
            if all(video_info.values()):
                writer.writerow([video_info["video_id"], video_info["title"], video_info["thumbnail_url"], video_info["published_time"], video_info["length"], video_info["view_count"], video_info["channel_name"], video_info["channel_url"]])

def main():
    input_csv = "input.csv" 
    token = "GPQxLsDwB4FSYFwG"  # Replace with your actual token
    output_folder = "output_data"
    os.makedirs(output_folder, exist_ok=True) 
    output_file = os.path.join(output_folder, "youtube_videos.csv")

    keywords = read_keywords_from_csv(input_csv)
    video_info_list = []
    for row in keywords:
        keyword = row["keyword"]
        depth = row.get("depth", 1)
        period = row.get("period", "overall")
        sorting = row.get("sorting", "relevance")
        cursor = row.get("cursor", "")  # Get cursor if provided
        response_data = fetch_youtube_videos(keyword, depth, period, sorting, token, cursor)
        if response_data:
            formatted_data = format_video_data(response_data)
            video_info_list.extend(formatted_data)

    save_videos_to_csv(video_info_list, output_file)
    print(f"Saved YouTube video info to {output_file}")

if __name__ == "__main__":
    main()
