import requests
import csv
import os
import pandas as pd

# Define the API endpoint
root = "https://ensembledata.com/apis"
endpoint = "/threads/keyword/search"

def read_keywords_from_csv(csv_path):
    df = pd.read_csv(csv_path, dtype=str)
    return df['keyword'].dropna().tolist()  # Assuming the CSV has a column named 'keyword'

def fetch_threads_info(keyword, token):
    params = {
        "name": keyword,
        "sorting": 0,
        "token": token
    }
    res = requests.get(root + endpoint, params=params)
    if res.status_code != 200:
        print(f"Error fetching threads data for keyword '{keyword}': {res.status_code}")
        return None
    return res.json()

def format_threads_data(data):
    threads_info = []
    for item in data.get("data", []):
        post = item.get("node", {}).get("thread", {}).get("thread_items", [{}])[0].get("post", {})
        user = post.get("user", {})
        
        thread_info = {
            "post_id": post.get("pk", ""),
            "user_id": user.get("id", ""),
            "username": user.get("username", ""),
            "profile_pic_url": user.get("profile_pic_url", ""),
            "is_verified": user.get("is_verified", False),
            "like_count": post.get("like_count", 0),
            "repost_count": post.get("text_post_app_info", {}).get("repost_count", 0),
            "reply_count": post.get("text_post_app_info", {}).get("direct_reply_count", 0),
            "content": post.get("caption", {}).get("text", ""),
            "media_type": post.get("media_type", None),
            "taken_at": post.get("taken_at", None),
           
        }
        threads_info.append(thread_info)
    return threads_info

def save_threads_to_csv(threads_info, filename):
    if not threads_info:
        print("No data to save.")
        return

    headers = ["post_id", "user_id", "username", "profile_pic_url", "like_count", "repost_count", "reply_count", "content", "media_type", "taken_at"]
    with open(filename, "w", newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(headers)
        for thread in threads_info:
            writer.writerow([thread["post_id"], thread["user_id"], thread["username"], thread["profile_pic_url"], thread["is_verified"], thread["like_count"], thread["repost_count"], thread["reply_count"], thread["content"], thread["media_type"], thread["taken_at"]])

def main():
    input_csv = "input.csv"  
    token = "SfFWgfc5TFLgQmWy"  
    output_folder = "output_data"
    os.makedirs(output_folder, exist_ok=True) 

    keywords = read_keywords_from_csv(input_csv)
    for keyword in keywords:
        response_data = fetch_threads_info(keyword, token)
        if response_data:
            formatted_data = format_threads_data(response_data)
            output_file = os.path.join(output_folder, f"{keyword}-search.csv")
            save_threads_to_csv(formatted_data, output_file)
            print(f"Saved threads info for keyword '{keyword}' to {output_file}")

if __name__ == "__main__":
    main()
