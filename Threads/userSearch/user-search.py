import requests
import csv
import os

# Define the API endpoint and parameters
root = "https://ensembledata.com/apis"
endpoint = "/threads/user/search"

def read_input_csv(csv_path):
    with open(csv_path, 'r') as f:
        reader = csv.DictReader(f)
        return [row['username'] for row in reader]

def fetch_user_info(username):
    params = {
        "name": username,
        "token": "SfFWgfc5TFLgQmWy"
    }
    res = requests.get(root + endpoint, params=params)
    if res.status_code != 200:
        print(f"Error fetching user data for {username}: {res.status_code} - {res.text}")
        return None
    return res.json()

def format_user_data(data):
    user_info = []
    for item in data.get("data", []):
        user = item.get("node", {})
        user_details = {
            "username": user.get("username", ""),
            "user_id": user.get("pk", ""),
            "full_name": user.get("full_name", ""),
            "profile_pic_url": user.get("profile_pic_url", ""),
            "is_verified": user.get("is_verified", False),
            "is_active_on_text_post_app": user.get("is_active_on_text_post_app", False),
            "has_onboarded_to_text_post_app": user.get("has_onboarded_to_text_post_app", False),
        }
        user_info.append(user_details)
    return user_info

def save_user_to_csv(user_info, filename):
    if not user_info:
        print("No data to save.")
        return

    headers = ["username", "user_id", "full_name", "profile_pic_url", "is_verified", "is_active_on_text_post_app", "has_onboarded_to_text_post_app"]
    with open(filename, "a", newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=headers)
        if f.tell() == 0: 
            writer.writeheader()
        for user in user_info:
            writer.writerow(user)

def main():
    input_csv = "input.csv"
    output_folder = "output_data"
    os.makedirs(output_folder, exist_ok=True)
    output_csv = os.path.join(output_folder, "user-search.csv")

    usernames = read_input_csv(input_csv)
    for username in usernames:
        response_data = fetch_user_info(username)
        if response_data:
            formatted_data = format_user_data(response_data)
            save_user_to_csv(formatted_data, output_csv)
            print(f"Saved user info for {username} to {output_csv}")

if __name__ == "__main__":
    main()
