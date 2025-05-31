import requests
import csv
import os
import pandas as pd

# Define the API endpoint
root = "https://ensembledata.com/apis"
endpoint = "/twitch/user/followers"

def read_usernames_from_csv(csv_path):
    df = pd.read_csv(csv_path, dtype=str)
    return df['username'].dropna().tolist()  # Assuming the CSV has a column named 'username'

def fetch_user_followers(username, token):
    params = {
        "username": username,
        "token": token
    }
    res = requests.get(root + endpoint, params=params)
    if res.status_code != 200:
        print(f"Error fetching followers for username '{username}': {res.status_code}")
        return None
    return res.json()

def save_followers_to_csv(followers_info_list, filename):
    if not followers_info_list:
        print("No follower data to save.")
        return

    headers = ["username", "followers_count"]
    with open(filename, "w", newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(headers)
        for followers_info in followers_info_list:
            writer.writerow([followers_info["username"], followers_info["followers_count"]])

def main():
    input_csv = "input.csv"  
    token = "SfFWgfc5TFLgQmWy"  
    output_folder = "output_data"
    os.makedirs(output_folder, exist_ok=True) 
    output_file = os.path.join(output_folder, "twitch_followers.csv")

    usernames = read_usernames_from_csv(input_csv)
    followers_info_list = []
    for username in usernames:
        response_data = fetch_user_followers(username, token)
        if response_data:
            followers_info = {
                "username": username,
                "followers_count": response_data.get("data", 0)
            }
            followers_info_list.append(followers_info)

    save_followers_to_csv(followers_info_list, output_file)
    print(f"Saved Twitch followers info to {output_file}")

if __name__ == "__main__":
    main()
