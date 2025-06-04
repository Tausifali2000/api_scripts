import csv
import os
import time
import requests
import pandas as pd

ROOT = "https://ensembledata.com/apis"
ENDPOINT = "/reddit/subreddit/posts"

# -------- helpers ----------------------------------------------------------- #
def load_tasks(csv_path: str):
    """Read subreddit_post_input.csv → list[dict] (name, sort, period, timestamp_limit)."""
    need = {"name", "sort", "period", "timestamp_limit"}
    df = pd.read_csv(csv_path, dtype=str)

    if not need.issubset(df.columns):
        raise ValueError(f"CSV must include columns: {', '.join(sorted(need))}")

    df["timestamp_limit"] = df["timestamp_limit"].astype(float)
    return df.to_dict("records")


def fetch_page(session, *, name, sort, period, cursor, token):
    """Single API request. Returns JSON or None on error."""
    params = {"name": name, "sort": sort, "period": period,
              "cursor": cursor, "token": token}

    r = session.get(ROOT + ENDPOINT, params=params, timeout=30)
    if r.status_code == 200:
        return r.json()

    print(f"❌ {name}: HTTP {r.status_code} – {r.text[:200]}")
    return None


def flatten_posts(api_json):
    """Turn raw API payload → list[dict] ready for CSV."""
    out = []
    for wrapper in api_json.get("data", {}).get("posts", []):
        if wrapper.get("kind") != "t3":
            continue

        d = wrapper["data"]
        author = d.get("author", "")
        out.append({
            "post_id":        d.get("id", ""),
            "post_url":       d.get("permalink", ""),
            "post_text":      d.get("selftext", ""),
            "post_time":      float(d.get("created_utc", 0)),
            "reaction_count": int(d.get("ups", 0)),
            "comment_count":  int(d.get("num_comments", 0)),
            "profile_id":     d.get("author_fullname", ""),
            "profile_name":   author,
            "profile_url":    f"https://www.reddit.com/user/{author}" if author else "",
        })
    return out


def save_csv(rows, subreddit_name):
    if not rows:
        print("⚠️  Nothing to write.")
        return

    folder = "subreddit_post"
    os.makedirs(folder, exist_ok=True)
    filename = f"{folder}/subreddit_post_{subreddit_name}.csv"
    
    with open(filename, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=rows[0].keys())
        w.writeheader()
        w.writerows(rows)

    print(f"✅ Saved {len(rows)} rows → {filename}")


# -------- main loop --------------------------------------------------------- #
def main():
    TOKEN = "kijGpadCYi0lZd8M"  # 🔑  paste your token here
    tasks = load_tasks("subreddit_post_input.csv")
    
    with requests.Session() as s:
        for t in tasks:
            name = t["name"]
            sort = t["sort"]
            period = t["period"]
            limit = float(t["timestamp_limit"])
            all_rows = []

            print(f"\n▶️  {name} | sort={sort} | period={period} | stop < {limit}")
            cursor = ""
            while True:
                page = fetch_page(s, name=name, sort=sort,
                                  period=period, cursor=cursor, token=TOKEN)
                if not page:
                    break

                rows = flatten_posts(page)
                if not rows:
                    print("⚠️  Empty page.")
                    break

                fresh = [r for r in rows if r["post_time"] >= limit]
                all_rows.extend(fresh)

                oldest = rows[-1]["post_time"]
                print(f"📦 got {len(rows):>2} posts "
                      f"({len(fresh):>2} kept) | oldest={oldest}")

                if any(r["post_time"] < limit for r in rows):
                    print("✅ encountered older post – stop paging")
                    break

                cursor = page.get("data", {}).get("nextCursor", "")
                if not cursor:
                    print("✅ no nextCursor – finished")
                    break

                time.sleep(0.5)

            save_csv(all_rows, name)


if __name__ == "__main__":
    main()
