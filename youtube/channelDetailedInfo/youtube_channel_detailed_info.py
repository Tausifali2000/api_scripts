import requests
import csv
import os

# API info
root = "https://ensembledata.com/apis"
endpoint = "/youtube/channel/detailed-info"

# Input and output paths
input_csv_file = 'youtube_channel_detailed_info_Input.csv'
output_folder = 'youtube_channel_detailed_info'  

def fetch_channel_info(browse_id):
    params = {
        "browseId": browse_id,
        "from_url": False,
        "get_additional_info": False,
        "token": "kijGpadCYi0lZd8M" 
    }
    res = requests.get(root + endpoint, params=params)
    if res.status_code != 200:
        print(f"Error fetching data for {browse_id}: {res.status_code}")
        return None
    return res.json()

def format_data(data):
    meta = data.get("data", {}).get("metadata", {}).get("aboutChannelViewModel", {})
    channel_info = {
        "description": meta.get("description", ""),
        "subscriber_count": meta.get("subscriberCountText", ""),
        "view_count": meta.get("viewCountText", ""),
        "joined_date": meta.get("joinedDateText", {}).get("content", ""),
        "video_count": meta.get("videoCountText", ""),
        "canonical_channel_url": meta.get("canonicalChannelUrl", ""),
        "channel_id": meta.get("channelId", ""),
        "links": []
    }

    for link in meta.get("links", []):
        link_data = link.get("channelExternalLinkViewModel", {})
        channel_info["links"].append({
            "title": link_data.get("title", {}).get("content", ""),
            "link": link_data.get("link", {}).get("content", "")
        })

    return channel_info

def save_to_csv(all_channel_info, filename):
    if not all_channel_info:
        print("No data to save.")
        return

    headers = [
        "description", "subscriber_count", "view_count", "joined_date",
        "video_count", "canonical_channel_url", "channel_id", "title", "link"
    ]

    with open(filename, "w", newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(headers)

        for channel_info in all_channel_info:
            if not channel_info["links"]:
                writer.writerow([
                    channel_info["description"],
                    channel_info["subscriber_count"],
                    channel_info["view_count"],
                    channel_info["joined_date"],
                    channel_info["video_count"],
                    channel_info["canonical_channel_url"],
                    channel_info["channel_id"],
                    "", ""
                ])
            else:
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
    if not os.path.exists(output_folder):
        os.makedirs(output_folder)

    with open(input_csv_file, mode='r', encoding='utf-8') as csvfile:
        csv_reader = csv.DictReader(csvfile)
        all_channel_info = []
        first_browse_id = None

        for i, row in enumerate(csv_reader):
            browse_id = row['browseId']
            if i == 0:
                first_browse_id = browse_id

            response_data = fetch_channel_info(browse_id)
            if response_data:
                all_channel_info.append(format_data(response_data))

    output_file_name = f"youtube_channel_detailed_info_{first_browse_id}.csv" if first_browse_id else "youtube_channel_detailed_info_output.csv"
    output_path = os.path.join(output_folder, output_file_name)

    save_to_csv(all_channel_info, output_path)
    print(f"✅ Saved all channel info to: {output_path}")

if __name__ == "__main__":
    main()
