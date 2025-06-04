import requests
import csv
import os
import pandas as pd
from datetime import datetime, timezone

# Define the API endpoint
root = "https://ensembledata.com/apis"
endpoint = "/reddit/subreddit/posts"

def read_csv(csv_path):
    try:
        df = pd.read_csv(csv_path, dtype=str)
        required_cols = ['name', 'sort', 'period', 'timestamp_limit']
        if not all(col in df.columns for col in required_cols):
            print(f"❌ Error: CSV must contain columns: {', '.join(required_cols)}")
            return []
        return df.to_dict(orient="records")
    except Exception as e:
        print(f"❌ Error reading CSV: {e}")
        return []

def fetch_posts(subreddit_name, sort, period, cursor, token):
    params = {
        "name": subreddit_name,
        "sort": sort,
        "period": period,
        "cursor": cursor,
        "token": token
    }

    res = requests.get(root + endpoint, params=params)

    if res.status_code == 200:
        return res.json()
    else:
        print(f"❌ Error fetching posts for {subreddit_name}: {res.status_code} - {res.text}")
        return None

def format_post_data(data):
    formatted_posts = []
    posts = data.get("data", {}).get("posts", [])

    for post in posts:
        if isinstance(post, dict) and post.get("kind") == "t3":
            post_data = post.get("data", {})
            author = post_data.get("author", "")
            created_utc = post_data.get("created_utc", 0)
            post_time = datetime.fromtimestamp(created_utc, timezone.utc).strftime('%H:%M:%S')

            formatted_post = {
                "post_id": post_data.get("id", ""),
                "post_url": post_data.get("permalink", ""),
                "post_text": post_data.get("selftext", ""),
                "post_time": post_time,
                "created_utc": created_utc,
                "reaction_count": post_data.get("ups", 0),
                "comment_count": post_data.get("num_comments", 0),
                "profile_id": post_data.get("author_fullname", ""),
                "profile_name": author,
                "profile_url": f"https://www.reddit.com/user/{author}" if author else ""
            }
            formatted_posts.append(formatted_post)

    return formatted_posts

def save_to_csv(posts_data):
    if not posts_data:
        print("⚠️ No posts to save.")
        return

    # Remove 'created_utc' from final CSV
    for post in posts_data:
        post.pop("created_utc", None)

    headers = posts_data[0].keys()
    os.makedirs("output_data", exist_ok=True)
    filename = "output_data/subreddit_post_output.csv"
    with open(filename, "w", newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=headers)
        writer.writeheader()
        writer.writerows(posts_data)
    print(f"✅ Saved {len(posts_data)} posts to {filename}")

def main():
    token = "GPQxLsDwB4FSYFwG"  # Replace with your token
    user_dict = read_csv("input.csv")

    all_posts = []

    for row in user_dict:
        subreddit_name = row["name"]
        sort = row["sort"]
        period = row["period"]
        timestamp_limit = datetime.strptime(row["timestamp_limit"], "%Y-%m-%d").timestamp()

        cursor = ""
        page_count = 0
        max_pages = 2  # Set a maximum number of pages to fetch

        while page_count < max_pages:
            print(f"\n🔄 [Page {page_count + 1}] Fetching from: {subreddit_name}, Cursor: {cursor}")
            res = fetch_posts(subreddit_name, sort, period, cursor, token)

            if not res:
                print("❌ API error or empty response.")
                break

            formatted_posts = format_post_data(res)
            if not formatted_posts:
                print("⚠️ No posts returned.")
                break

            all_posts.extend(formatted_posts)

            last_post_timestamp = formatted_posts[-1]["created_utc"]
            print(f"🕒 Last post timestamp: {last_post_timestamp}, Limit: {timestamp_limit}")
            print(f"📦 Fetched {len(formatted_posts)} posts on this page.")

            if float(last_post_timestamp) < timestamp_limit:
                print(f"✅ Reached timestamp limit: {row['timestamp_limit']}")
                break

            # Update cursor for the next request
            cursor = res.get("data", {}).get("nextCursor", "")
            print(f"Next Cursor: {cursor}")  # Check the cursor value

            if not cursor or len(formatted_posts) < 25:  # Check if cursor is empty or fewer posts than expected
                print("✅ No more cursor or fewer posts fetched than expected. Pagination complete.")
                break

            page_count += 1

    save_to_csv(all_posts)

if __name__ == "__main__":
    main()
