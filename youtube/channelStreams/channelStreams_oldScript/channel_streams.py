import requests
import csv
import os

# Define the API endpoint
root = "https://ensembledata.com/apis"
endpoint = "/youtube/channel/streams"

def read_params_from_csv(filename):
    params_list = []
    with open(filename, "r", encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            params_list.append(row)
    return params_list

def fetch_channel_streams(params):
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

def format_stream_data(data):
    streams_info = []
    streams = data.get("data", {}).get("streams", [])
    
    for stream in streams:
        stream_data = stream.get("richItemRenderer", {}).get("content", {}).get("videoRenderer", {})
        stream_info = {
            "video_id": stream_data.get("videoId", ""),
            "title": stream_data.get("title", {}).get("runs", [{}])[0].get("text", ""),
            "thumbnail": stream_data.get("thumbnail", {}).get("thumbnails", [{}])[0].get("url", ""),
            "description": stream_data.get("descriptionSnippet", {}).get("runs", [{}])[0].get("text", ""),
            "view_count": stream_data.get("viewCountText", {}).get("runs", [{}])[0].get("text", "N/A"),
            "watching_count": stream_data.get("viewCountText", {}).get("runs", [{}])[1].get("text", "N/A") if len(stream_data.get("viewCountText", {}).get("runs", [])) > 1 else "N/A"
        }
        streams_info.append(stream_info)

    return streams_info

def save_to_csv(channel_info, streams_info, filename):
    if not streams_info:
        print("No stream data to save.")
        return

    headers = ["channel_url", "channel_title", "channel_description", "channel_thumbnail", "tags", "video_id", "title", "thumbnail", "description", "view_count", "watching_count"]
    with open(filename, "w", newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(headers)
        for stream in streams_info:
            writer.writerow([
                channel_info["url"],
                channel_info["title"],
                channel_info["description"],
                channel_info["thumbnail"],
                ", ".join(channel_info["tags"]),
                stream["video_id"],
                stream["title"],
                stream["thumbnail"],
                stream["description"],
                stream["view_count"],
                stream["watching_count"]
            ])

def main():
    params_list = read_params_from_csv("input.csv")
    
    # Ensure output directory exists
    output_dir = "output"
    os.makedirs(output_dir, exist_ok=True)
    
    # Set the output filename
    output_filename = os.path.join(output_dir, "channel_videos_input.csv")
    
    # Initialize a list to hold all stream data
    all_streams_info = []
    channel_info = None
    
    for params in params_list:
        browse_id = params['browseId']
        request_params = {
            "browseId": browse_id,
            "depth": 1,
            "token": "GPQxLsDwB4FSYFwG"  # Your actual token
        }
        response_data = fetch_channel_streams(request_params)
        if response_data:
            if channel_info is None:
                channel_info = format_channel_data(response_data) 
            streams_info = format_stream_data(response_data)
            all_streams_info.extend(streams_info)  

    # Save all collected data to a single CSV file
    save_to_csv(channel_info, all_streams_info, output_filename)
    print(f"Saved channel and stream details to {output_filename}")

if __name__ == "__main__":
    main()
