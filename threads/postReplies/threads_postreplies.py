import requests
import csv
import os
import pandas as pd
from pathlib import Path

# --------------------------------------------------------------------------- #
ROOT = "https://ensembledata.com/apis"
ENDPOINT = "/threads/post/replies"
TOKEN = "kijGpadCYi0lZd8M"                       # fixed token
INPUT_CSV = "threads_postreplies_input.csv"      # column: post_id
OUT_DIR = Path("threads_postreplies")            # output folder
# --------------------------------------------------------------------------- #

def read_post_ids(csv_path: str):
    """Return a list of post IDs from the input CSV."""
    df = pd.read_csv(csv_path, dtype=str)
    return df["post_id"].dropna().tolist()

def fetch_replies(post_id: str):
    """Call the API to get replies for a single post."""
    params = {"id": post_id, "token": TOKEN}
    r = requests.get(ROOT + ENDPOINT, params=params, timeout=20)
    if r.status_code == 200:
        return r.json()
    print(f"❌ Error {r.status_code} for post {post_id}: {r.text[:100]} …")
    return None

def flatten_replies(api_json):
    """Convert the nested API structure → list[dict] ready for CSV."""
    rows = []
    for node in api_json.get("data", []):
        for thread_item in node.get("node", {}).get("thread_items", []):
            post = thread_item.get("post", {})
            user = post.get("user", {})
            rows.append({
                "reply_id":      post.get("pk", ""),
                "username":      user.get("username", ""),
                "user_id":       user.get("pk", ""),
                "profile_pic_url": user.get("profile_pic_url", ""),
                "is_verified":   user.get("is_verified", False),
                "reply_text":    (
                    post.get("text_post_app_info", {})
                        .get("text_fragments", {})
                        .get("fragments", [{}])[0]
                        .get("plaintext", "")
                ),
                "like_count":    post.get("like_count", 0),
                "timestamp":     post.get("taken_at", ""),
                "caption":       post.get("caption", {}).get("text", ""),
                "media_type":    post.get("media_type", "")
            })
    return rows

def save_csv(rows, post_id: str):
    """Save list[dict] to CSV in OUT_DIR with sanitized filename."""
    if not rows:
        print(f"⚠️  No replies for post {post_id}.")
        return

    OUT_DIR.mkdir(exist_ok=True)
    file_path = OUT_DIR / f"threads_postreplies_{post_id}.csv"

    with file_path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=rows[0].keys())
        writer.writeheader()
        writer.writerows(rows)

    print(f"✅ Saved {len(rows)} replies → {file_path}")

def main():
    post_ids = read_post_ids(INPUT_CSV)
    if not post_ids:
        print("❌ No post IDs found in the input CSV.")
        return

    for pid in post_ids:
        print(f"\n▶️ Fetching replies for post ID: {pid}")
        data = fetch_replies(pid)
        if data:
            flattened = flatten_replies(data)
            save_csv(flattened, pid)

if __name__ == "__main__":
    main()
