import requests
import csv
import os
import pandas as pd

# API endpoint and token
ROOT = "https://ensembledata.com/apis"
ENDPOINT = "/reddit/post/comments"
TOKEN = "kijGpadCYi0lZd8M"  # Fixed token to use in all scripts

# Read input CSV
def read_csv(csv_path):
    df = pd.read_csv(csv_path, dtype=str)
    return df.to_dict(orient="records")

# Fetch comments from API
def fetch_comments(permalink, token):
    params = {"permalink": permalink, "token": token}
    res = requests.get(ROOT + ENDPOINT, params=params)
    if res.status_code == 200:
        return res.json()
    else:
        print(f"❌ Error fetching comments for {permalink}: {res.status_code} - {res.text}")
        return None

# Format comments and replies
def format_comment_data(data):
    formatted_comments = []
    comments = data.get("data", {}).get("comments", [])

    for comment in comments:
        if isinstance(comment, dict) and comment.get("kind") == "t1":
            d = comment.get("data", {})
            author = d.get("author", "")
            formatted = {
                "comment_id": d.get("id", ""),
                "comment_text": d.get("body", ""),
                "comment_url": d.get("permalink", ""),
                "comment_time": d.get("created_utc", ""),
                "comment_reaction_count": d.get("score", 0),
                "commenter_id": d.get("author_fullname", ""),
                "commenter_name": author,
                "profile_url": f"https://www.reddit.com/user/{author}" if author else "",
                "reply_count": 0,
                "replies": []
            }

            # Safely extract replies
            replies = d.get("replies")
            if isinstance(replies, dict):
                reply_items = replies.get("data", {}).get("children", [])
                for reply in reply_items:
                    if reply.get("kind") == "t1":
                        rd = reply.get("data", {})
                        formatted["replies"].append({
                            "reply_id": rd.get("id", ""),
                            "reply_text": rd.get("body", ""),
                            "reply_url": rd.get("permalink", ""),
                            "replier_name": rd.get("author", ""),
                            "reply_reaction_count": rd.get("score", 0)
                        })

            formatted["reply_count"] = len(formatted["replies"])
            formatted_comments.append(formatted)

    return formatted_comments

# Save formatted comments to CSV
def save_to_csv(comments_data, filename):
    if not comments_data:
        print("⚠️ No comments to save.")
        return

    os.makedirs(os.path.dirname(filename), exist_ok=True)

    with open(filename, "w", newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow([
            "comment_id", "comment_text", "comment_url", "comment_time", "comment_reaction_count",
            "commenter_id", "commenter_name", "profile_url",
            "reply_id", "reply_text", "reply_url", "replier_name", "reply_reaction_count"
        ])

        for c in comments_data:
            if c["replies"]:
                for r in c["replies"]:
                    writer.writerow([
                        c["comment_id"], c["comment_text"], c["comment_url"], c["comment_time"], c["comment_reaction_count"],
                        c["commenter_id"], c["commenter_name"], c["profile_url"],
                        r["reply_id"], r["reply_text"], r["reply_url"], r["replier_name"], r["reply_reaction_count"]
                    ])
            else:
                writer.writerow([
                    c["comment_id"], c["comment_text"], c["comment_url"], c["comment_time"], c["comment_reaction_count"],
                    c["commenter_id"], c["commenter_name"], c["profile_url"],
                    "", "", "", "", ""
                ])

    print(f"✅ Saved comments → {filename}")

# Main script
def main():
    input_path = "post_comment_input.csv"
    rows = read_csv(input_path)

    for row in rows:
        permalink = row.get("permalink", "")
        if not permalink:
            continue

        print(f"\n▶️ Fetching comments from post: {permalink}")
        result = fetch_comments(permalink, TOKEN)
        if result:
            formatted = format_comment_data(result)

            # Extract just the post ID from permalink
            post_id = permalink.rstrip("/").split("/")[-2] if permalink.endswith("/") else permalink.split("/")[-1]
            output_path = f"post_comment/post_comment_{post_id}.csv"

            save_to_csv(formatted, output_path)

if __name__ == "__main__":
    main()
