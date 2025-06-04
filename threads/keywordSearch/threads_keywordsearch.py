import requests
import csv
import os
import pandas as pd

# API endpoint
ROOT = "https://ensembledata.com/apis"
ENDPOINT = "/threads/keyword/search"

# Fixed token
TOKEN = "kijGpadCYi0lZd8M"

# Input and output setup
INPUT_CSV = "threads_keywordsearch_input.csv"
OUTPUT_DIR = "threads_keywordsearch"

def read_keywords_from_csv(csv_path):
    """Read keywords from CSV file."""
    df = pd.read_csv(csv_path, dtype=str)
    return df['keyword'].dropna().tolist()  # Ensure no NaNs

def fetch_threads_info(keyword, token):
    """Call the Threads API with the given keyword."""
    params = {
        "name": keyword,
        "sorting": 0,
        "token": token
    }
    try:
        res = requests.get(ROOT + ENDPOINT, params=params, timeout=20)
        if res.status_code == 200:
            return res.json()
        else:
            print(f"❌ Error for keyword '{keyword}': HTTP {res.status_code} – {res.text[:100]}")
    except Exception as e:
        print(f"❌ Request failed for '{keyword}': {e}")
    return None

def format_threads_data(data):
    """Flatten the thread response JSON."""
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
            "media_type": post.get("media_type", ""),
            "taken_at": post.get("taken_at", "")
        }
        threads_info.append(thread_info)
    return threads_info

def save_threads_to_csv(threads_info, keyword):
    """Save the list of threads to a CSV file."""
    if not threads_info:
        print(f"⚠️ No threads found for keyword '{keyword}'.")
        return

    os.makedirs(OUTPUT_DIR, exist_ok=True)
    filename = f"{OUTPUT_DIR}/threads_keywordsearch_{keyword.replace(' ', '_')}.csv"

    with open(filename, "w", newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=threads_info[0].keys())
        writer.writeheader()
        writer.writerows(threads_info)

    print(f"✅ Saved {len(threads_info)} posts for '{keyword}' → {filename}")

def main():
    keywords = read_keywords_from_csv(INPUT_CSV)
    if not keywords:
        print("❌ No keywords found in input CSV.")
        return

    for keyword in keywords:
        print(f"\n▶️ Fetching threads for keyword: {keyword}")
        data = fetch_threads_info(keyword, TOKEN)
        if data:
            formatted = format_threads_data(data)
            save_threads_to_csv(formatted, keyword)

if __name__ == "__main__":
    main()
