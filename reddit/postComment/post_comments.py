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

def format_comment_data(data):
    formatted_comments = []
    
    comments = data.get("data", {}).get("comments", [])

    for comment in comments:
        if isinstance(comment, dict) and comment.get("kind") == "t1":  # Only process actual comments
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
                "reply_count": len(comment_data.get("replies", {}).get("data", {}).get("children", [])) if isinstance(comment_data.get("replies"), dict) else 0,
                "replies": []  # Initialize replies list
            }
            formatted_comments.append(formatted_comment)

            # Process nested replies
            replies = comment_data.get("replies")
            if isinstance(replies, dict):
                replies_children = replies.get("data", {}).get("children", [])
                for reply in replies_children:
                    if reply.get("kind") == "t1":  # Ensure it's a reply
                        reply_data = reply.get("data", {})
                        formatted_reply = {
                            "reply_id": reply_data.get("id", ""),
                            "reply_text": reply_data.get("body", ""),
                            "reply_url": reply_data.get("permalink", ""),
                            "replier_name": reply_data.get("author", ""),
                            "reply_reaction_count": reply_data.get("score", 0)
                        }
                        formatted_comment["replies"].append(formatted_reply)

    return formatted_comments

def save_to_csv(comments_data, filename):
    if not comments_data:
        print("No comments to save.")
        return

    # Create a CSV file with comments and replies
    with open(filename, "w", newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        # Write header
        writer.writerow(["comment_id", "comment_text", "comment_url", "comment_time", "comment_reaction_count", 
                         "commenter_id", "commenter_name", "profile_url", 
                         "reply_id", "reply_text", "reply_url", "replier_name", "reply_reaction_count"])
        
        for comment in comments_data:
            # Write the main comment
            for reply in comment["replies"]:
                writer.writerow([
                    comment["comment_id"],
                    comment["comment_text"],
                    comment["comment_url"],
                    comment["comment_time"],
                    comment["comment_reaction_count"],
                    comment["commenter_id"],
                    comment["commenter_name"],
                    comment["profile_url"],
                    reply["reply_id"],
                    reply["reply_text"],
                    reply["reply_url"],
                    reply["replier_name"],
                    reply["reply_reaction_count"]
                ])
            # If there are no replies, write the comment with empty reply fields
            if not comment["replies"]:
                writer.writerow([
                    comment["comment_id"],
                    comment["comment_text"],
                    comment["comment_url"],
                    comment["comment_time"],
                    comment["comment_reaction_count"],
                    comment["commenter_id"],
                    comment["commenter_name"],
                    comment["profile_url"],
                    "", "", "", "", ""
                ])

def main():
    token = "SfFWgfc5TFLgQmWy"  # Replace with your actual token
    user_dict = read_csv("input.csv")

    for row in user_dict:
        permalink = row["permalink"]
        print(f"Fetching comments from post: {permalink}")
        res = fetch_comments(permalink, token)

        if res:
            formatted_comments = format_comment_data(res)
            # Create output filename based on permalink
            output_filename = f"output_data/post_comments.csv"
            save_to_csv(formatted_comments, output_filename)
            print(f"Saved comments for post {permalink} to {output_filename}")

if __name__ == "__main__":
    main()
