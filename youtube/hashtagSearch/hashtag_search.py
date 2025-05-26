import requests
import csv

# Define the API endpoint and parameters
root = "https://ensembledata.com/apis"
endpoint = "/youtube/hashtag/search"
params = {
    "name": "magic",  
    "depth": 1,
    "only_shorts": False,
    "token": "SfFWgfc5TFLgQmWy"  # Replace with your actual token
}

def fetch_hashtag_data():
    res = requests.get(root + endpoint, params=params)
    if res.status_code != 200:
        print(f"Error fetching data: {res.status_code}")
        return None
    return res.json()

def format_data(data):
    hashtag_info = {
        "page_title": data.get("data", {}).get("info", {}).get("pageTitle", ""),
        "video_count": data.get("data", {}).get("info", {}).get("content", {}).get("pageHeaderViewModel", {}).get("metadata", {}).get("contentMetadataViewModel", {}).get("metadataRows", [{}])[0].get("metadataParts", [{}])[0].get("text", {}).get("content", ""),
        "videos": []
    }

    videos = data.get("data", {}).get("videos", [])
    for video in videos:
        video_data = video.get("richItemRenderer", {}).get("content", {}).get("reelItemRenderer", {})
        video_info = {
            "video_id": video_data.get("videoId", ""),
            "headline": video_data.get("headline", {}).get("simpleText", ""),
            "thumbnail": video_data.get("thumbnail", {}).get("thumbnails", [{}])[0].get("url", ""),
            "view_count": video_data.get("viewCountText", {}).get("simpleText", "")
        }
        hashtag_info["videos"].append(video_info)

    return hashtag_info

def save_to_csv(hashtag_info, filename):
    if not hashtag_info["videos"]:
        print("No data to save.")
        return

    headers = ["page_title", "video_count", "video_id", "headline", "thumbnail", "view_count"]
    with open(filename, "w", newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(headers)
        for video in hashtag_info["videos"]:
            writer.writerow([hashtag_info["page_title"], hashtag_info["video_count"], video["video_id"], video["headline"], video["thumbnail"], video["view_count"]])

def main():
    response_data = fetch_hashtag_data()
    if response_data:
        formatted_data = format_data(response_data)
        save_to_csv(formatted_data, "hashtag_search.csv")
        print(f"Saved hashtag data to hashtag_search.csv")

if __name__ == "__main__":
    main()
