import requests
import csv
import os
import pandas as pd

# Define the API endpoint and parameters
root = "https://ensembledata.com/apis"
endpoint = "/threads/post/replies"

def read_post_ids_from_csv(csv_path):
    df = pd.read_csv(csv_path, dtype=str)
    return df['post_id'].dropna().tolist()

def fetch_post_replies(post_id, token):
    params = {
        "id": post_id,
        "token": token
    }
    res = requests.get(root + endpoint, params=params)
    if res.status_code != 200:
        print(f"Error fetching replies for post ID {post_id}: {res.status_code} - {res.text}")
        return None
    return res.json()

def format_replies_data(data):
    replies_info = []
    for item in data.get("data", []):
        thread_items = item.get("node", {}).get("thread_items", [])
        for thread_item in thread_items:
            post = thread_item.get("post", {})
            user = post.get("user", {})
            reply_details = {
                "reply_id": post.get("pk", ""),
                "username": user.get("username", ""),
                "user_id": user.get("pk", ""),
                "profile_pic_url": user.get("profile_pic_url", ""),
                "is_verified": user.get("is_verified", False),
                "reply_text": post.get("text_post_app_info", {}).get("text_fragments", {}).get("fragments", [{}])[0].get("plaintext", ""),
                "like_count": post.get("like_count", 0),
                "timestamp": post.get("taken_at", 0),
                "caption": post.get("caption", {}).get("text", ""), 
                "media_type": post.get("media_type", None)  
            }
            replies_info.append(reply_details)
    return replies_info

def save_replies_to_csv(replies_info, filename):
    if not replies_info:
        print("No replies to save.")
        return

    headers = ["reply_id", "username", "user_id", "profile_pic_url", "is_verified", "reply_text", "like_count", "timestamp", "caption", "media_type"]
    with open(filename, "w", newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(headers)
        for reply in replies_info:
            writer.writerow([reply["reply_id"], reply["username"], reply["user_id"], reply["profile_pic_url"], reply["is_verified"], reply["reply_text"], reply["like_count"], reply["timestamp"], reply["caption"], reply["media_type"]])

def main():
    input_csv = "input.csv" 
    token = "B3jQjPFTs8Y88lXj" 
    output_folder = "output_data"
    os.makedirs(output_folder, exist_ok=True) 
    output_file = os.path.join(output_folder, "post_replies.csv")

    post_ids = read_post_ids_from_csv(input_csv)
    for post_id in post_ids:
        response_data = fetch_post_replies(post_id, token)
        if response_data:
            formatted_data = format_replies_data(response_data)
            save_replies_to_csv(formatted_data, output_file)
            print(f"Saved replies for post ID {post_id} to {output_file}")

if __name__ == "__main__":
    main()
