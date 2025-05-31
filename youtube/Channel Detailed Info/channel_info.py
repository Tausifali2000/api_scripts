import requests
import csv
import os

root = "https://ensembledata.com/apis"
endpoint = "/youtube/channel/detailed-info"

input_csv_file = 'input.csv'
output_folder = 'output_data'

def fetch_channel_info(browse_id):
    params = {
        "browseId": browse_id,
        "from_url": False,
        "get_additional_info": False,
        "token": "GPQxLsDwB4FSYFwG" 
    }
    res = requests.get(root + endpoint, params=params)
    if res.status_code != 200:
        print(f"Error fetching data for {browse_id}: {res.status_code}")
        return None
    return res.json()

def format_data(data):
    channel_info = {
        "description": data.get("data", {}).get("metadata", {}).get("aboutChannelViewModel", {}).get("description", ""),
        "subscriber_count": data.get("data", {}).get("metadata", {}).get("aboutChannelViewModel", {}).get("subscriberCountText", ""),
        "view_count": data.get("data", {}).get("metadata", {}).get("aboutChannelViewModel", {}).get("viewCountText", ""),
        "joined_date": data.get("data", {}).get("metadata", {}).get("aboutChannelViewModel", {}).get("joinedDateText", {}).get("content", ""),
        "video_count": data.get("data", {}).get("metadata", {}).get("aboutChannelViewModel", {}).get("videoCountText", ""),
        "canonical_channel_url": data.get("data", {}).get("metadata", {}).get("aboutChannelViewModel", {}).get("canonicalChannelUrl", ""),
        "channel_id": data.get("data", {}).get("metadata", {}).get("aboutChannelViewModel", {}).get("channelId", ""),
        "links": []
    }

    links = data.get("data", {}).get("metadata", {}).get("aboutChannelViewModel", {}).get("links", [])
    for link in links:
        link_data = link.get("channelExternalLinkViewModel", {})
        link_info = {
            "title": link_data.get("title", {}).get("content", ""),
            "link": link_data.get("link", {}).get("content", ""),
        }
        channel_info["links"].append(link_info)

    return channel_info

def save_to_csv(all_channel_info, filename):
    if not all_channel_info:
        print("No data to save.")
        return

    headers = ["description", "subscriber_count", "view_count", "joined_date", "video_count", "canonical_channel_url", "channel_id", "title", "link"]
    with open(filename, "w", newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(headers)
        for channel_info in all_channel_info:
            for link in channel_info["links"]:
                writer.writerow([
                    channel_info["description"], 
                    channel_info["subscriber_count"], 
                    channel_info["view_count"], 
                    channel_info["joined_date"], 
                    channel_info["video_count"], 
                    channel_info["canonical_channel_url"], 
                    channel_info["channel_id"], 
                    link["title"], 
                    link["link"]
                ])

def main():
    # Make sure output folder exists
    if not os.path.exists(output_folder):
        os.makedirs(output_folder)

    with open(input_csv_file, mode='r', encoding='utf-8') as csvfile:
        csv_reader = csv.DictReader(csvfile)
        all_channel_info = []

        for row in csv_reader:
            browse_id = row['browseId']
            response_data = fetch_channel_info(browse_id)
            if response_data:
                formatted_data = format_data(response_data)
                all_channel_info.append(formatted_data)

    output_file_name = os.path.join(output_folder, "channel_info.csv")
    save_to_csv(all_channel_info, output_file_name)
    print(f"Saved all channel info to {output_file_name}")

if __name__ == "__main__":
    main()
