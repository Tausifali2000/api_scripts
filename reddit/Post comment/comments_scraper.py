import requests
import csv
import os

TOKEN = "CoSVZABHepXLpV5Q"
root = "https://ensembledata.com/apis"
endpoint = "/reddit/post/comments"
params = {
    "permalink": "/r/SkincareAddiction/comments/8pv3gg/ba_finished_my_5_month_course_of_accutane_just_in/",
    "token": TOKEN
}

def fetch_comments():
    res = requests.get(root + endpoint, params=params)
    if res.status_code != 200:
        print(f"Error fetching data: {res.status_code}")
        return None
    return res.json()

def format_data(data):
    post_data = {
       
        "profile_url": f"https://www.reddit.com/user/{data.get('data', {}).get('author', '')}",
      
    }

    comments = data.get("data", {}).get("comments", [])
    formatted_comments = []

    for comment in comments:
        comment_data = comment.get("data", {})
        formatted_comment = {
            "comment_id": comment_data.get("name", ""),
            "comment_text": comment_data.get("body", ""),
            "comment_url": comment_data.get("permalink", ""),
            "comment_time": comment_data.get("created_utc", ""),
            "comment_reaction_count": comment_data.get("score", 0),
            "commenter_id": comment_data.get("author_fullname", ""),
            "commenter_name": comment_data.get("author", ""),
            "commenter_url": f"https://www.reddit.com/user/{comment_data.get('author', '')}",
            "reply_count": len(format_replies(comment_data.get("replies")))  # Count replies
        }
        formatted_comments.append(formatted_comment)

    return post_data, formatted_comments

def format_replies(replies_data):
    replies = []
    # Check if replies_data is a dictionary and has the expected structure
    if isinstance(replies_data, dict) and replies_data.get("kind") == "Listing":
        for child in replies_data.get("data", {}).get("children", []):
            child_data = child.get("data", {})
            link_id = child_data.get("link_id", "")
            parent_id = child_data.get("parent_id", "")
            
            # Ensure link_id and parent_id are formatted correctly
            post_id = link_id.split('_')[1] if link_id and '_' in link_id else ""
            comment_id = parent_id.split('_')[1] if parent_id and '_' in parent_id else ""

            replies.append({
                "post_id": post_id,  # Extract post_id from link_id
                "comment_id": comment_id,  # Extract comment_id from parent_id
                "reply_id": child_data.get("name", ""),
                "reply_text": child_data.get("body", ""),
                "reply_url": child_data.get("permalink", ""),
                "reply_time": child_data.get("created_utc", ""),
                "reply_reaction_count": child_data.get("score", 0),
                "replier_id": child_data.get("author_fullname", ""),
                "replier_name": child_data.get("author", ""),
                "replier_url": f"https://www.reddit.com/user/{child_data.get('author', '')}"
            })
    return replies

def save_to_csv(post_data, comments_data, filename):
    if not comments_data:
        print("No data to save.")
        return

    headers = list(post_data.keys()) + list(comments_data[0].keys())
    with open(filename, "w", newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=headers)
        writer.writeheader()
        for comment in comments_data:
            writer.writerow({**post_data, **comment})

def main():
    response_data = fetch_comments()
    if response_data:
        post_data, formatted_comments = format_data(response_data)
        save_to_csv(post_data, formatted_comments, "reddit_comments.csv")
        print(f"Saved {len(formatted_comments)} comments to reddit_comments.csv")

if __name__ == "__main__":
    main()
