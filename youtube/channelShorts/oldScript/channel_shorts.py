import requests
import csv
import os

# Define the API endpoint
root = "https://ensembledata.com/apis"
endpoint = "/youtube/channel/shorts"

def read_params_from_csv(filename):
    params_list = []
    with open(filename, "r", encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            params_list.append(row)
    return params_list

def fetch_channel_shorts(params):
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

def format_shorts_data(data):
    shorts_info = []
    shorts = data.get("data", {}).get("shorts", [])
    
    for short in shorts:
        short_data = short.get("richItemRenderer", {}).get("content", {}).get("reelItemRenderer", {})
        short_info = {
            "video_id": short_data.get("videoId", ""),
            "headline": short_data.get("headline", {}).get("simpleText", ""),
            "thumbnail": short_data.get("thumbnail", {}).get("thumbnails", [{}])[0].get("url", ""),
            "view_count": short_data.get("viewCountText", {}).get("simpleText", ""),
        }
        shorts_info.append(short_info)

    return shorts_info

def save_to_csv(channel_info, shorts_info, filename):
    if not shorts_info:
        print("No shorts data to save.")
        return

    headers = ["channel_url", "channel_title", "channel_description", "channel_thumbnail", "tags", "video_id", "headline", "thumbnail", "view_count"]
    with open(filename, "w", newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(headers)
        for short in shorts_info:
            writer.writerow([
                channel_info["url"],
                channel_info["title"],
                channel_info["description"],
                channel_info["thumbnail"],
                ", ".join(channel_info["tags"]),
                short["video_id"],
                short["headline"],
                short["thumbnail"],
                short["view_count"],
              
            ])

def main():
    params_list = read_params_from_csv("youtube_channelShorts_input.csv")
    
    # Ensure output directory exists
    output_dir = "output"
    os.makedirs(output_dir, exist_ok=True)
    
    # Set the output filename
    output_filename = os.path.join(output_dir, "channel_shorts_output.csv")
    
    # Initialize a list to hold all shorts data
    all_shorts_info = []
    channel_info = None
    
    for params in params_list:
        browse_id = params['browseId']
        request_params = {
            "browseId": browse_id,
            "depth": 1,
            "token": "B3jQjPFTs8Y88lXj"  # Replace with your actual token
        }
        response_data = fetch_channel_shorts(request_params)
        if response_data:
            if channel_info is None:
                channel_info = format_channel_data(response_data)  # Get channel info only once
            shorts_info = format_shorts_data(response_data)
            all_shorts_info.extend(shorts_info)  # Collect all shorts info

    # Save all collected data to a single CSV file
    save_to_csv(channel_info, all_shorts_info, output_filename)
    print(f"Saved channel and shorts details to {output_filename}")

if __name__ == "__main__":
    main()
