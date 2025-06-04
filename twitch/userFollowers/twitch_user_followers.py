import requests
import csv
import os
import pandas as pd

# API details
ROOT   = "https://ensembledata.com/apis"
ENDPT  = "/twitch/user/followers"
TOKEN  = "kijGpadCYi0lZd8M"

def read_usernames(csv_path: str):
    """Return a list of usernames from the input CSV."""
    df = pd.read_csv(csv_path, dtype=str)
    return df["username"].dropna().tolist()

def fetch_followers(username: str):
    """Call the Twitch follower endpoint for one username."""
    params = {"username": username, "token": TOKEN}
    r = requests.get(ROOT + ENDPT, params=params)
    if r.status_code == 200:
        return r.json()
    print(f"❌ Error {r.status_code} fetching '{username}'")
    return None

def save_to_csv(row: dict, username: str, out_dir: str):
    """Save a single row to its own CSV file."""
    os.makedirs(out_dir, exist_ok=True)
    file_path = os.path.join(out_dir, f"twitch_user_followers_{username}.csv")
    with open(file_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=row.keys())
        writer.writeheader()
        writer.writerow(row)
    print(f"✅ Saved → {file_path}")

def main():
    input_csv   = "twitch_user_followers_input.csv"   
    output_dir  = "twitch_user_followers_output"

    usernames = read_usernames(input_csv)
    if not usernames:
        print("No usernames found.")
        return

    for name in usernames:
        print(f"Fetching followers for: {name}")
        resp = fetch_followers(name)
        if resp:
            row = {
                "username":        name,
                "followers_count": resp.get("data", 0)
            }
            save_to_csv(row, name, output_dir)

if __name__ == "__main__":
    main()
