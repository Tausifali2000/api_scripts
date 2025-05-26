import requests
import csv

# Define the API endpoint and parameters
root = "https://ensembledata.com/apis"
endpoint = "/youtube/channel/detailed-info"
params = {
    "browseId": "UCnQghMm3Z164JFhScQYFTBw",  
    "from_url": False,
    "get_additional_info": False,
    "token": "SfFWgfc5TFLgQmWy"  # Replace with your actual token
}

def fetch_channel_info():
    res = requests.get(root + endpoint, params=params)
    if res.status_code != 200:
        print(f"Error fetching data: {res.status_code}")
        return None
    return res.json()

def format_data(data):
    channel_info = {
        "description": data.get("data", {}).get("metadata", {}).get("aboutChannelViewModel", {}).get("description", ""),
        "subscriber_count": data.get("data", {}).get("metadata", {}).get("aboutChannelViewModel", {}).get("subscriberCountText", ""),
        "view_count": data.get("data", {}).get("metadata", {}).get("aboutChannelViewModel", {}).get("viewCountText", ""),
        "joined_date": data.get("data", {}).get("metadata", {}).get("aboutChannelViewModel", {}).get("joinedDateText", {}).get("content", ""),
        "video_count": data.get("data", {}).get("metadata", {}).get("aboutChannelViewModel", {}).get("videoCountText", ""),
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

def save_to_csv(channel_info, filename):
    if not channel_info["links"]:
        print("No data to save.")
        return

    headers = ["description", "subscriber_count", "view_count", "joined_date", "video_count", "title", "link", ]
    with open(filename, "w", newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(headers)
        for link in channel_info["links"]:
            writer.writerow([channel_info["description"], channel_info["subscriber_count"], channel_info["view_count"], channel_info["joined_date"], channel_info["video_count"], link["title"], link["link"], ])

def main():
    response_data = fetch_channel_info()
    if response_data:
        formatted_data = format_data(response_data)
        save_to_csv(formatted_data, "channel_info.csv")
        print(f"Saved channel info to channel_info.csv")

if __name__ == "__main__":
    main()
