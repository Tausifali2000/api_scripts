import requests
import csv
import os
import pandas as pd
from pathlib import Path

# Define API endpoint
root = "https://ensembledata.com/apis"
endpoint = "/threads/user/info"

def read_user_ids_from_csv(csv_path):
    df = pd.read_csv(csv_path, dtype=str)
    return df['user_id'].dropna().tolist()

def fetch_user_info(user_id, token):
    params = {
        "id": user_id,
        "token": token
    }
    res = requests.get(root + endpoint, params=params)
    if res.status_code != 200:
        print(f"Error fetching user data for ID {user_id}: {res.status_code}")
        return None
    return res.json()

def format_user_data(data):
    return {
        "user_id": data.get("data", {}).get("id", ""),
        "username": data.get("data", {}).get("username", ""),
        "full_name": data.get("data", {}).get("full_name", ""),
        "profile_pic_url": data.get("data", {}).get("profile_pic_url", ""),
        "follower_count": data.get("data", {}).get("follower_count", 0),
        "biography": data.get("data", {}).get("biography", ""),
    }

def save_user_to_csv(user_info, user_id):
    output_folder = Path("threads_userinfo")
    output_folder.mkdir(exist_ok=True)
    filename = output_folder / f"threads_userinfo-{user_id}.csv"

    headers = list(user_info.keys())
    with open(filename, "w", newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=headers)
        writer.writeheader()
        writer.writerow(user_info)

    print(f"Saved user info for {user_id} to {filename}")

def main():
    input_csv = "threads_userinfo_input.csv"
    token = "kijGpadCYi0lZd8M"  # your token
    
    user_ids = read_user_ids_from_csv(input_csv)
    for user_id in user_ids:
        response_data = fetch_user_info(user_id, token)
        if response_data:
            formatted_data = format_user_data(response_data)
            save_user_to_csv(formatted_data, user_id)

if __name__ == "__main__":
    main()
