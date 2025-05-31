import requests
import csv
import os
import pandas as pd

# Define the API endpoint
root = "https://ensembledata.com/apis"
endpoint = "/threads/user/info"

def read_user_ids_from_csv(csv_path):
    df = pd.read_csv(csv_path, dtype=str)
    return df['user_id'].dropna().tolist() 

def fetch_user_info(user_id, token):
    params = {
        "id": user_id,
        "token": token
    }
    res = requests.get(root + endpoint, params=params)
    if res.status_code != 200:
        print(f"Error fetching user data for ID {user_id}: {res.status_code}")
        return None
    return res.json()

def format_user_data(data):
    user_info = {
        "user_id": data.get("data", {}).get("id", ""),
        "username": data.get("data", {}).get("username", ""),
        "full_name": data.get("data", {}).get("full_name", ""),
        "profile_pic_url": data.get("data", {}).get("profile_pic_url", ""),
        "follower_count": data.get("data", {}).get("follower_count", 0),
        "biography": data.get("data", {}).get("biography", ""),
        "is_verified": data.get("data", {}).get("is_verified", False),
    }
    return user_info

def save_user_info_to_csv(user_info_list, filename):
    if not user_info_list:
        print("No user data to save.")
        return

    headers = ["user_id", "username", "full_name", "profile_pic_url", "follower_count", "biography", "is_verified"]
    with open(filename, "w", newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(headers)
        for user_info in user_info_list:
            writer.writerow([user_info["user_id"], user_info["username"], user_info["full_name"], user_info["profile_pic_url"], user_info["follower_count"], user_info["biography"], user_info["is_verified"]])

def main():
    input_csv = "input.csv"
    token = "SfFWgfc5TFLgQmWy" 
    output_folder = "output_data"
    os.makedirs(output_folder, exist_ok=True) 
    output_file = os.path.join(output_folder, "user_info.csv")

    user_ids = read_user_ids_from_csv(input_csv)
    user_info_list = []
    for user_id in user_ids:
        response_data = fetch_user_info(user_id, token)
        if response_data:
            formatted_data = format_user_data(response_data)
            user_info_list.append(formatted_data)

    save_user_info_to_csv(user_info_list, output_file)
    print(f"Saved user info to {output_file}")

if __name__ == "__main__":
    main()
