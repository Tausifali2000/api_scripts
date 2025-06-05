import requests
import csv
import os
import pandas as pd

# Define the API endpoint
root = "https://ensembledata.com/apis"
endpoint = "/twitch/search"

def read_keywords_from_csv(csv_path):
    df = pd.read_csv(csv_path, dtype=str)
    return df['keyword'].dropna().tolist()  

def fetch_twitch_search_results(keyword, token):
    params = {
        "keyword": keyword,
        "depth": 1,
        "type": "videos",
        "token": token
    }
    res = requests.get(root + endpoint, params=params)
    if res.status_code != 200:
        print(f"Error fetching search results for keyword '{keyword}': {res.status_code}")
        return None
    return res.json()

def format_search_data(data):
    search_info_list = []
    for video in data.get("data", {}).get("videos", []):
        item = video.get("item", {})
        search_info = {
            "trackingID": video.get("trackingID", ""),
            "video_id": item.get("id", ""),
            "title": item.get("title", ""),
            "createdAt": item.get("createdAt", ""),
            "owner_id": item.get("owner", {}).get("id", ""),
            "owner_displayName": item.get("owner", {}).get("displayName", ""),
            "game_name": item.get("game", {}).get("name", ""),
            "viewCount": item.get("viewCount", 0),
        }
        search_info_list.append(search_info)
    return search_info_list

def save_to_csv(data_list, filename):
    if not data_list:
        print("No data to save.")
        return

    headers = [
        "trackingID", "video_id", "title", "createdAt",
        "owner_id", "owner_displayName", "game_name", "viewCount"
    ]
    with open(filename, "w", newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=headers)
        writer.writeheader()
        for data in data_list:
            writer.writerow(data)

def main():
    input_csv = "twitch_keyword_search_input.csv"
    output_folder = "twitch_keyword_search_output"
    os.makedirs(output_folder, exist_ok=True)
    output_file = os.path.join(output_folder, "twitch_keyword_search_output.csv")
    token = "kijGpadCYi0lZd8M"

    keywords = read_keywords_from_csv(input_csv)
    all_search_data = []

    for keyword in keywords:
        print(f"Searching Twitch for keyword: {keyword}")
        response_data = fetch_twitch_search_results(keyword, token)
        if response_data:
            formatted_data = format_search_data(response_data)
            all_search_data.extend(formatted_data)

    save_to_csv(all_search_data, output_file)
    print(f"Saved Twitch search results to {output_file}")

if __name__ == "__main__":
    main()
