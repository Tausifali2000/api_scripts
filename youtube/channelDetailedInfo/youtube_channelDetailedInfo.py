import requests
import os
import pandas as pd
import csv
from dotenv import load_dotenv 
from datetime import datetime, timedelta, timezone
import re

load_dotenv()

TOKEN = os.getenv("ENSEMBLE_DATA_TOKEN")

csv_path = "youtube_channelDetailedInfo_input.csv"

def convert_joined_date_to_epoch(joined_str):
    try:
        # Remove the "Joined " prefix
        clean_date_str = joined_str.replace("Joined ", "").strip()
        # Parse the date
        dt = datetime.strptime(clean_date_str, "%b %d, %Y")
        return int(dt.timestamp())
    except Exception as e:
        print(f"[ERROR] Failed to convert joined date '{joined_str}' to epoch: {e}")
        return None

def read_csv(csv_path):
    df = pd.read_csv(csv_path, dtype=str)  
    rows = df.to_dict(orient="records")   
    return rows


def req_youtube_channelDetailedInfo(browseId):
    root = "https://ensembledata.com/apis"
    endpoint = "/youtube/channel/detailed-info"
    params = {
        "browseId": browseId,
        "token": TOKEN,
        
    }
    res = requests.get(root + endpoint, params=params)
    
    return res


def format_channelDetailedInfo(res, browseId):
    try:
        data = res.json().get("data", {})
        aboutChannel = data.get("metadata", {}).get("aboutChannelViewModel", {})

        profile_url = aboutChannel.get("canonicalChannelUrl", "")
        post_text = aboutChannel.get("description", "")
        joined_info = aboutChannel.get("joinedDateText", {}).get("content", "")
        post_time = convert_joined_date_to_epoch(joined_info)

        formatted_channelDetails = {
            "profile_id": browseId,
            "profile_url": profile_url,
            "post_text": post_text,
            "post_time": post_time,
        }

        return formatted_channelDetails

    except Exception as e:
        print(f"[ERROR] Failed to format channel info: {e}")
        return None

    

def download_channelDetails_results(channel_dict):
    output_dir = "youtube_channelDetails_output"
    output_file = os.path.join(output_dir, "youtube_channelDetails_output.csv")

    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    all_results = []

    for row in channel_dict:
        browseId = row.get("browseId", "").strip()
        if not browseId:
            print("[WARNING] Skipping row with missing 'browseId'")
            continue

        print(f"[INFO] Fetching channel details for: {browseId}")
        try:
            res = req_youtube_channelDetailedInfo(browseId)
            if res.status_code != 200:
                print(f"[ERROR] Failed to fetch channel data for '{browseId}' (Status: {res.status_code})")
                continue

            formatted_data = format_channelDetailedInfo(res, browseId)

            if formatted_data:
                all_results.append(formatted_data)
                print(f"[SUCCESS] Fetched channel info for: {browseId}")
            else:
                print(f"[INFO] No data formatted for: {browseId}")

        except Exception as e:
            print(f"[ERROR] Exception for '{browseId}': {e}")

    # Write to CSV
    if all_results:
        df = pd.DataFrame(all_results)
        if os.path.exists(output_file):
            df.to_csv(output_file, mode='a', index=False, encoding="utf-8", header=False)
            print(f"[APPEND] Appended {len(df)} new rows to '{output_file}'")
        else:
            df.to_csv(output_file, index=False, encoding="utf-8")
            print(f"[CREATE] Created file and saved {len(df)} rows to '{output_file}'")
    else:
        print("[INFO] No channel details to write.")




channel_dict = read_csv(csv_path)  # Must contain 'browseId' column
download_channelDetails_results(channel_dict)