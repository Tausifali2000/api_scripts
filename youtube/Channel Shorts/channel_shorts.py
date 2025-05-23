import requests
import csv

# Define the API endpoint and parameters
root = "https://ensembledata.com/apis"
endpoint = "/youtube/channel/shorts"
params = {
    "browseId": "UClAa0YLrW4MaIKOWaeZR9Xg", 
    "depth": 1,
    "token": "SfFWgfc5TFLgQmWy"  # Replace with your actual token
}

def fetch_channel_shorts():
    res = requests.get(root + endpoint, params=params)
    if res.status_code != 200:
        print(f"Error fetching data: {res.status_code}")
        return None
    return res.json()

def format_data(data):
    shorts_info = []
    shorts = data.get("data", {}).get("shorts", [])
    
    for short in shorts:
        short_data = short.get("richItemRenderer", {}).get("content", {}).get("reelItemRenderer", {})
        short_info = {
            "video_id": short_data.get("videoId", ""),
            "headline": short_data.get("headline", {}).get("simpleText", ""),
            "thumbnail": short_data.get("thumbnail", {}).get("thumbnails", [{}])[0].get("url", ""),
            "view_count": short_data.get("viewCountText", {}).get("simpleText", ""),
            "published_time": short_data.get("navigationEndpoint", {}).get("reelWatchEndpoint", {}).get("videoId", ""),
            "length": short_data.get("lengthText", {}).get("simpleText", "")
        }
        shorts_info.append(short_info)

    return shorts_info

def save_to_csv(shorts_info, filename):
    if not shorts_info:
        print("No data to save.")
        return

    headers = ["video_id", "headline", "thumbnail", "view_count", "published_time", "length"]
    with open(filename, "w", newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(headers)
        for short in shorts_info:
            writer.writerow([short["video_id"], short["headline"], short["thumbnail"], short["view_count"], short["published_time"], short["length"]])

def main():
    response_data = fetch_channel_shorts()
    if response_data:
        formatted_data = format_data(response_data)
        save_to_csv(formatted_data, "channel_shorts.csv")
        print(f"Saved channel shorts to channel_shorts.csv")

if __name__ == "__main__":
    main()
