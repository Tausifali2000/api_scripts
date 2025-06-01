import requests
import csv
import os
import pandas as pd

# Define the API endpoint
root = "https://ensembledata.com/apis"
endpoint = "/twitch/search"

def read_keywords_from_csv(csv_path):
    df = pd.read_csv(csv_path, dtype=str)
    return df['keyword'].dropna().tolist()  # Assuming the CSV has a column named 'keyword'

def fetch_twitch_videos(keyword, token):
    params = {
        "keyword": keyword,
        "depth": 1,
        "type": "videos",
        "token": token
    }
    res = requests.get(root + endpoint, params=params)
    if res.status_code != 200:
        print(f"Error fetching videos for keyword '{keyword}': {res.status_code}")
        return None
    return res.json()

def format_video_data(data):
    video_info_list = []
    for video in data.get("data", {}).get("videos", []):
        item = video.get("item", {})
        video_info = {
            "trackingID": video.get("trackingID", ""),
            "video_id": item.get("id", ""),
            "title": item.get("title", ""),
            "createdAt": item.get("createdAt", ""),
            "owner_id": item.get("owner", {}).get("id", ""),
            "owner_displayName": item.get("owner", {}).get("displayName", ""),
            "game_name": item.get("game", {}).get("name", ""),
            "lengthSeconds": item.get("lengthSeconds", 0),
            "viewCount": item.get("viewCount", 0),
            "previewThumbnailURL": item.get("previewThumbnailURL", ""),
        }
        video_info_list.append(video_info)
    return video_info_list

def save_videos_to_csv(video_info_list, filename):
    if not video_info_list:
        print("No video data to save.")
        return

    headers = ["trackingID", "video_id", "title", "createdAt", "owner_id", "owner_displayName", "game_name", "lengthSeconds", "viewCount", "previewThumbnailURL"]
    with open(filename, "w", newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(headers)
        for video_info in video_info_list:
            writer.writerow([video_info["trackingID"], video_info["video_id"], video_info["title"], video_info["createdAt"], video_info["owner_id"], video_info["owner_displayName"], video_info["game_name"], video_info["lengthSeconds"], video_info["viewCount"], video_info["previewThumbnailURL"]])

def main():
    input_csv = "input.csv"  
    token = "SfFWgfc5TFLgQmWy" 
    output_folder = "output_data"
    os.makedirs(output_folder, exist_ok=True) 
    output_file = os.path.join(output_folder, "twitch_videos.csv")

    keywords = read_keywords_from_csv(input_csv)
    video_info_list = []
    for keyword in keywords:
        response_data = fetch_twitch_videos(keyword, token)
        if response_data:
            formatted_data = format_video_data(response_data)
            video_info_list.extend(formatted_data)

    save_videos_to_csv(video_info_list, output_file)
    print(f"Saved Twitch video info to {output_file}")

if __name__ == "__main__":
    main()
