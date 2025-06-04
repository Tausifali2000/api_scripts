import requests
import csv
import os
import pandas as pd

# Define the API endpoint and parameters
root = "https://ensembledata.com/apis"
endpoint = "/threads/user/posts"
token = "kijGpadCYi0lZd8M"  

def read_user_ids_from_csv(csv_path):
    df = pd.read_csv(csv_path, dtype=str)
    return df['id'].dropna().tolist()  

def fetch_user_posts(user_id):
    params = {
        "id": user_id,
        "chunk_size": 10,
        "token": token
    }
    res = requests.get(root + endpoint, params=params)
    if res.status_code != 200:
        print(f"Error fetching user posts for ID {user_id}: {res.status_code} - {res.text}")
        return None
    return res.json()

def format_posts_data(data):
    posts_info = []
    for item in data.get("data", []):
        thread_items = item.get("node", {}).get("thread_items", [])
        for thread_item in thread_items:
            post = thread_item.get("post", {})
            user = post.get("user", {})
            post_details = {
                "post_id": post.get("pk", ""),
                "username": user.get("username", ""),
                "user_id": user.get("pk", ""),
                "profile_pic_url": user.get("profile_pic_url", ""),
                "is_verified": user.get("is_verified", False),
                "caption": post.get("caption", {}).get("text", "") if post.get("caption") else "",
                "like_count": post.get("like_count", 0),
                "timestamp": post.get("taken_at", 0),
                "media_type": post.get("media_type", ""),
                "code": post.get("code", ""),
            }
            posts_info.append(post_details)
    return posts_info

def save_posts_to_csv(posts_info, filename):
    if not posts_info:
        print("No posts to save.")
        return

    headers = ["post_id", "username", "user_id", "profile_pic_url", "is_verified", "caption", "like_count", "timestamp", "media_type", "code"]
    with open(filename, "w", newline='', encoding='utf-8') as f:  
        writer = csv.DictWriter(f, fieldnames=headers)
        writer.writeheader()
        for post in posts_info:
            writer.writerow(post)

def main():
    input_csv = "threads_userposts_input.csv"  # input CSV file with user IDs in 'id' column
    output_folder = "threads_userposts_output"  
    os.makedirs(output_folder, exist_ok=True)

    user_ids = read_user_ids_from_csv(input_csv)
    for user_id in user_ids:
        print(f"Fetching posts for user ID: {user_id}")
        response_data = fetch_user_posts(user_id)
        if response_data:
            formatted_data = format_posts_data(response_data)
            output_csv = os.path.join(output_folder, f"threads_userposts_{user_id}.csv")
            save_posts_to_csv(formatted_data, output_csv)
            print(f"Saved user posts for ID {user_id} to {output_csv}")

if __name__ == "__main__":
    main()
