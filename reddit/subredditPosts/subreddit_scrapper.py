import requests
import csv
import json

# Define the API endpoint and parameters
root = "https://ensembledata.com/apis"
endpoint = "/reddit/subreddit/posts"
params = {
    "name": "SkincareAddiction",
    "sort": "new",
    "period": "hour",
    "cursor": "",
    "token": "HDvPycfJaKJlA6IE"  # Ensure this token is valid
}

def fetch_posts():
    res = requests.get(root + endpoint, params=params)
    if res.status_code != 200:
        print(f"Error fetching data: {res.status_code}")
        return None
    return res.json()

def format_post_data(data):
    posts = data.get("data", [])
    formatted_posts = []

    for post in posts:
        if isinstance(post, dict):  # Ensure post is a dictionary
            post_data = post.get("data", {})
            formatted_post = {
                "post_id": post_data.get("id", ""),
                "post_url": post_data.get("url", ""),
                "post_text": post_data.get("selftext", ""),
                "post_time": post_data.get("created_utc", ""),
                "profile_id": post_data.get("author_fullname", ""),
                "profile_name": post_data.get("author", ""),
                "profile_url": f"https://www.reddit.com/user/{post_data.get('author', '')}",
                "reaction_count": post_data.get("ups", 0),
                "comment_count": post_data.get("num_comments", 0)
            }
            formatted_posts.append(formatted_post)

    return formatted_posts

def fetch_comments(post_id):
    comments_endpoint = f"/reddit/post/comments"
    comment_params = {
        "permalink": f"/r/SkincareAddiction/comments/{post_id}/",
        "token": "HDvPycfJaKJlA6IE"  # Ensure this token is valid
    }
    res = requests.get(root + comments_endpoint, params=comment_params)
    if res.status_code != 200:
        print(f"Error fetching comments for post {post_id}: {res.status_code}")
        return None
    return res.json()

def format_comments_data(data, post_id):
    comments = data.get("data", {}).get("comments", [])
    formatted_comments = []

    for comment in comments:
        comment_data = comment.get("data", {})
        replies = comment_data.get("replies", {})
        reply_children = replies.get("data", {}).get("children", []) if isinstance(replies, dict) else []

        formatted_comment = {
            "post_id": post_id,
            "comment_id": comment_data.get("name", ""),
            "comment_text": comment_data.get("body", ""),
            "comment_url": comment_data.get("permalink", ""),
            "comment_time": comment_data.get("created_utc", ""),
            "comment_reaction_count": comment_data.get("score", 0),
            "commenter_id": comment_data.get("author_fullname", ""),
            "commenter_name": comment_data.get("author", ""),
            "commenter_url": f"https://www.reddit.com/user/{comment_data.get('author', '')}",
            "reply_count": len(reply_children)
        }
        formatted_comments.append(formatted_comment)

    return formatted_comments

def save_to_csv(posts_data, comments_data, filename):
    if not posts_data:
        print("No posts to save.")
        return

    post_headers = posts_data[0].keys()
    comment_headers = comments_data[0].keys() if comments_data else []

    headers = list(post_headers | comment_headers)  # Union of headers

    with open(filename, "w", newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=headers)
        writer.writeheader()
        for post in posts_data:
            writer.writerow(post)
        for comment in comments_data:
            writer.writerow(comment)

def main():
    response_data = fetch_posts()
    if response_data:
        print(json.dumps(response_data, indent=4))  # Print the response data for debugging
        formatted_posts = format_post_data(response_data)
        all_comments = []

        for post in formatted_posts:
            comments_data = fetch_comments(post["post_id"])
            if comments_data:
                formatted_comments = format_comments_data(comments_data, post["post_id"])
                all_comments.extend(formatted_comments)

        save_to_csv(formatted_posts, all_comments, "reddit_data.csv")
        print(f"Saved {len(formatted_posts)} posts and {len(all_comments)} comments to reddit_data.csv")

if __name__ == "__main__":
    main()
