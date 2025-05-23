import requests
import csv

# Define the API endpoint and parameters
root = "https://ensembledata.com/apis"
endpoint = "/youtube/channel/streams"
params = {
    "browseId": "UChBQgieUidXV1CmDxSdRm3g",  
    "depth": 1,
    "token": "SfFWgfc5TFLgQmWy"  # Replace with your actual token
}

def fetch_channel_streams():
    res = requests.get(root + endpoint, params=params)
    if res.status_code != 200:
        print(f"Error fetching data: {res.status_code}")
        return None
    return res.json()

def format_data(data):
    streams_info = []
    streams = data.get("data", {}).get("streams", [])
    
    for stream in streams:
        stream_data = stream.get("richItemRenderer", {}).get("content", {}).get("videoRenderer", {})
        view_count_text = stream_data.get("viewCountText", {}).get("runs", [])
        
        # Safely get the view count text
        if len(view_count_text) > 1:
            view_count = view_count_text[0].get("text", "") + " " + view_count_text[1].get("text", "")
        elif len(view_count_text) == 1:
            view_count = view_count_text[0].get("text", "")
        else:
            view_count = "0 views"  # Default value if no view count is available

        stream_info = {
            "video_id": stream_data.get("videoId", ""),
            "title": stream_data.get("title", {}).get("runs", [{}])[0].get("text", ""),
            "thumbnail": stream_data.get("thumbnail", {}).get("thumbnails", [{}])[0].get("url", ""),
            "view_count": view_count,
            "description": stream_data.get("descriptionSnippet", {}).get("runs", [{}])[0].get("text", ""),
            "live_status": "LIVE" if "LIVE" in stream_data.get("thumbnailOverlays", [{}])[0].get("thumbnailOverlayTimeStatusRenderer", {}).get("text", {}).get("runs", [{}])[0].get("text", "") else "Not Live"
        }
        streams_info.append(stream_info)

    return streams_info


def save_to_csv(streams_info, filename):
    if not streams_info:
        print("No data to save.")
        return

    headers = ["video_id", "title", "thumbnail", "view_count", "description", "live_status"]
    with open(filename, "w", newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(headers)
        for stream in streams_info:
            writer.writerow([stream["video_id"], stream["title"], stream["thumbnail"], stream["view_count"], stream["description"], stream["live_status"]])

def main():
    response_data = fetch_channel_streams()
    if response_data:
        formatted_data = format_data(response_data)
        save_to_csv(formatted_data, "channel_streams.csv")
        print(f"Saved channel streams to channel_streams.csv")

if __name__ == "__main__":
    main()
