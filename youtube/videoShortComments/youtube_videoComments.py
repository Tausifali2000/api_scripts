import requests
import os
import pandas as pd
import csv
from dotenv import load_dotenv 
from datetime import datetime, timedelta, timezone
import re

load_dotenv()

TOKEN = os.getenv("ENSEMBLE_DATA_TOKEN")

csv_path = "youtube_videoComments_input.csv"

def convert_relative_time_to_epoch(relative_time: str) -> int:
  
   
    now = datetime.now(timezone.utc)

    time_map = {
        'second': 'seconds',
        'seconds': 'seconds',
        'minute': 'minutes',
        'minutes': 'minutes',
        'hour': 'hours',
        'hours': 'hours',
        'day': 'days',
        'days': 'days',
        'week': 'weeks',
        'weeks': 'weeks',
        'month': 'days',   
        'months': 'days',
        'year': 'days',    
        'years': 'days'
    }

    match = re.match(r"(\d+)\s+(second|seconds|minute|minutes|hour|hours|day|days|week|weeks|month|months|year|years)\s+ago", relative_time.strip())
    if not match:
        return int(now.timestamp())

    value, unit = int(match.group(1)), match.group(2)

    if unit in ['month', 'months']:
        delta = timedelta(days=value * 30)
    elif unit in ['year', 'years']:
        delta = timedelta(days=value * 365)
    else:
        delta = timedelta(**{time_map[unit]: value})

    target_time = now - delta
    return int(target_time.timestamp())


    if not view_str:
        return 0

    view_str = view_str.strip().upper().replace(",", "").replace(" VIEWS", "")

    try:
        if view_str.endswith("K"):
            return int(float(view_str[:-1]) * 1_000)
        elif view_str.endswith("M"):
            return int(float(view_str[:-1]) * 1_000_000)
        elif view_str.endswith("B"):
            return int(float(view_str[:-1]) * 1_000_000_000)
        else:
            return int(view_str)  # fallback for raw numbers like "12345"
    except:
        return 0


def read_csv(csv_path):
    df = pd.read_csv(csv_path, dtype=str)  
    rows = df.to_dict(orient="records")   
    return rows


def req_youtube_videoComments(videoId, cursor=""):
    root = "https://ensembledata.com/apis"
    endpoint = "/youtube/video/comments"
    params = {
        "id": videoId,
        "cursor": cursor,
        "token": TOKEN,
        
    }
    res = requests.get(root + endpoint, params=params)
    
    return res


def format_video_comments(res, videoId, comment_count=None):
    try:
        data = res.json().get("data", {})
        info = data.get("info", {})
        comments = data.get("comments", [])
        next_cursor = data.get("nextCursor")

        # Extract comment_count only on first page
        if comment_count is None:
            try:
                continuation_items = info.get("reloadContinuationItemsCommand", {}).get("continuationItems", [])
                header = continuation_items[0].get("commentsHeaderRenderer", {})
                runs = header.get("countText", {}).get("runs", [])
                comment_count_str = runs[0].get("text", "0") if runs else "0"
                comment_count = int(comment_count_str.replace(",", ""))
                print("[DEBUG] comment_count:", comment_count)
            except Exception as e:
                print(f"[WARNING] Could not extract comment_count: {e}")
                comment_count = len(comments)  # fallback

    except Exception as e:
        print(f"[ERROR] Failed to parse comments JSON: {e}")
        return [], None, comment_count

    if not comments:
        return [], next_cursor, comment_count

    formatted_comments = []

    for i, item in enumerate(comments):
        try:
            print(f"[DEBUG] Processing comment #{i+1}")
            comment_thread = item.get("commentThreadRenderer")
            if not comment_thread:
                continue
            comment = comment_thread.get("comment")
            if not comment:
                continue

            row = {
                "post_id": videoId,
                "post_url": f"https://www.youtube.com/watch?v={videoId}",
                "comment_count": comment_count,
                "comment_id": comment.get("properties", {}).get("commentId", ""),
                "comment_text": comment.get("properties", {}).get("content", {}).get("content", ""),
                "comment_time": convert_relative_time_to_epoch(comment.get("properties", {}).get("publishedTime", "")),
                "comment_reaction_count": comment.get("toolbar", {}).get("likeCountLiked", ""),
                "commenter_name": comment.get("author", {}).get("displayName", "").lstrip("@"),
                "commenter_url": f"https://www.youtube.com/channel/{comment.get('author', {}).get('channelId', '')}",
                "commenter_id": comment.get("author", {}).get("channelId", ""),
                "reply_count": int(comment.get("toolbar", {}).get("replyCount") or 0)
            }

            formatted_comments.append(row)

        except Exception as e:
            print(f"[ERROR] Failed to parse comment item: {e}")
            continue

    return formatted_comments, next_cursor, comment_count


    

def download_videoComments_results(video_dict):
    output_dir = "youtube_videoComments_output"
    output_file = os.path.join(output_dir, "youtube_videoComments_output.csv")

    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    all_results = []

    for row in video_dict:
        video_id = row.get("videoId", "").strip()
        if not video_id:
            print("[WARNING] Skipping row with missing 'videoId'")
            continue

        print(f"[INFO] Fetching comments for video: {video_id}")
        cursor = ""
        total_comments = 0
        has_more_comments = True
        comment_count = None  # Track only once per video

        while has_more_comments:
            res = req_youtube_videoComments(video_id, cursor)
            
            if res.status_code != 200:
                print(f"[ERROR] Failed to fetch comments for '{video_id}' (Status: {res.status_code})")
                break

            formatted_data, next_cursor, comment_count = format_video_comments(res, video_id, comment_count)

            if not formatted_data:
                print(f"[INFO] No comments found in this batch for video '{video_id}'")
                break

            total_comments += len(formatted_data)
            all_results.extend(formatted_data)
            print(f"[SUCCESS] Fetched {len(formatted_data)} comments (Total: {total_comments})")
            print(f"[DEBUG] next_cursor: {next_cursor}")
            if not next_cursor:
                print(f"[DONE] All comments fetched for video '{video_id}'")
                has_more_comments = False
            else:
                cursor = next_cursor

    # Write to CSV
    if all_results:
        df = pd.DataFrame(all_results)

        if os.path.exists(output_file):
            df.to_csv(output_file, mode='a', index=False, encoding="utf-8", header=False)
            print(f"[APPEND] Appended {len(df)} new rows to '{output_file}'")
        else:
            df.to_csv(output_file, index=False, encoding="utf-8")
            print(f"[CREATE] Created file and saved {len(df)} rows to '{output_file}'")
    else:
        print("[INFO] No results to write.")




user_dict = read_csv(csv_path)
download_videoComments_results(user_dict)