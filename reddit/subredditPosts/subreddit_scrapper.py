import requests
import csv
import os
import pandas as pd

# Define the API endpoint
root = "https://ensembledata.com/apis"
endpoint = "/reddit/subreddit/posts"

def read_csv(csv_path):
    try:
        df = pd.read_csv(csv_path, dtype=str)
        if 'name' not in df.columns:
            print(f"Error: CSV file {csv_path} must have a 'name' column.")
            return []
        rows = df.to_dict(orient="records")
        return rows
    except FileNotFoundError:
        print(f"Error: CSV file {csv_path} not found.")
        return []
    except Exception as e:
        print(f"Error reading CSV: {e}")
        return []

def fetch_posts(subreddit_name, token):
    params = {
        "name": subreddit_name,
        "sort": "new",
        "period": "hour",
        "token": token
    }
    
    res = requests.get(root + endpoint, params=params)
    
    if res.status_code == 200:
        return res.json()
    else:
        print(f"Error fetching posts for {subreddit_name}: {res.status_code} - {res.text}")
        return None

def format_post_data(data, formatted_posts=None):
    if formatted_posts is None:
        formatted_posts = []
    
    posts = data.get("data", {}).get("posts", [])

    for post in posts:
        if isinstance(post, dict) and post.get("kind") == "t3":  # Only process actual posts
            post_data = post.get("data", {})
            author = post_data.get("author", "")
            formatted_post = {
                "post_id": post_data.get("id", ""),
                "post_url": post_data.get("url", ""),
                "post_text": post_data.get("selftext", ""),
                "post_time": post_data.get("created_utc", ""),
                "reaction_count": post_data.get("ups", 0),
                "comment_count": post_data.get("num_comments", 0),
                "profile_id": post_data.get("author_fullname", ""),
                "profile_name": author,
                "profile_url": f"https://www.reddit.com/user/{author}" if author else ""
            }
            formatted_posts.append(formatted_post)

    return formatted_posts

def save_to_csv(posts_data, filename):
    if not posts_data:
        print("No posts to save.")
        return

    headers = posts_data[0].keys()
    with open(filename, "w", newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=headers)
        writer.writeheader()
        writer.writerows(posts_data)

def main():
    token = "jsK2yBd12gZlW1PI"  # Try "VSKSPtmy3XRmbROO" if 401 error
    user_dict = read_csv("input.csv")

    all_posts = []
    for row in user_dict:
        subreddit_name = row["name"]
        print(f"Fetching posts from subreddit: {subreddit_name}")
        res = fetch_posts(subreddit_name, token)

        if res:
            formatted_posts = format_post_data(res)
            all_posts.extend(formatted_posts)

    # Create output folder if it doesn't exist
    os.makedirs("output_data", exist_ok=True)

    # Save to CSV
    save_to_csv(all_posts, "output_data/reddit_posts_output.csv")
    print(f"Saved {len(all_posts)} posts to output_data/reddit_posts_output.csv")

if __name__ == "__main__":
    main()