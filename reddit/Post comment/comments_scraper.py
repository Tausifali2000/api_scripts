import requests
import csv
import os
import pandas as pd

# Define the API endpoint
root = "https://ensembledata.com/apis"
endpoint = "/reddit/post/comments"

def read_csv(csv_path):
    df = pd.read_csv(csv_path, dtype=str)
    rows = df.to_dict(orient="records")
    return rows

def fetch_comments(permalink, token):
    params = {
        "permalink": permalink,
        "token": token
    }
    
    res = requests.get(root + endpoint, params=params)
    
    if res.status_code == 200:
        return res.json()
    else:
        print(f"Error fetching comments for {permalink}: {res.status_code} - {res.text}")
        return None

def format_comment_data(data, formatted_comments=None):
    if formatted_comments is None:
        formatted_comments = []
    
    comments = data.get("data", {}).get("comments", [])

    for comment in comments:
        if isinstance(comment, dict) and comment.get("kind") == "t1": 
            comment_data = comment.get("data", {})
            author = comment_data.get("author", "")
            formatted_comment = {
                "comment_id": comment_data.get("id", ""),
                "comment_text": comment_data.get("body", ""),
                "comment_url": comment_data.get("permalink", ""),
                "comment_time": comment_data.get("created_utc", ""),
                "comment_reaction_count": comment_data.get("score", 0),
                "commenter_id": comment_data.get("author_fullname", ""),
                "commenter_name": author,
                "profile_url": f"https://www.reddit.com/user/{author}" if author else "",
                "commenter_url": f"https://www.reddit.com/user/{author}" if author else "",
                "reply_count": len(comment_data.get("replies", {}).get("data", {}).get("children", [])) if isinstance(comment_data.get("replies"), dict) else 0
            }
            formatted_comments.append(formatted_comment)

            # Recursively process nested replies
            replies = comment_data.get("replies")
            if isinstance(replies, dict):
                replies_children = replies.get("data", {}).get("children", [])
                if replies_children:
                    format_comment_data({"data": {"comments": replies_children}}, formatted_comments)

    return formatted_comments

def save_to_csv(comments_data, filename):
    if not comments_data:
        print("No comments to save.")
        return

    headers = comments_data[0].keys()
    with open(filename, "w", newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=headers)
        writer.writeheader()
        writer.writerows(comments_data)

def main():
    token = "jsK2yBd12gZlW1PI"  # Replace with your actual token
    user_dict = read_csv("input.csv")

    all_comments = []
    for row in user_dict:
        permalink = row["permalink"]
        print(f"Fetching comments from post: {permalink}")
        res = fetch_comments(permalink, token)

        if res:
            formatted_comments = format_comment_data(res)
            all_comments.extend(formatted_comments)

    # Create output folder if it doesn't exist
    os.makedirs("output_data", exist_ok=True)

    # Save to CSV
    save_to_csv(all_comments, "output_data/reddit_comments_output.csv")
    print(f"Saved {len(all_comments)} comments to output_data/reddit_comments_output.csv")

if __name__ == "__main__":
    main()