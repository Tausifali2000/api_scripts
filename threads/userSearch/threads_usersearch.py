import requests
import csv
import os

# API endpoint and token
root = "https://ensembledata.com/apis"
endpoint = "/threads/user/search"
token = "kijGpadCYi0lZd8M"  # your token here

def read_input_csv(csv_path):
    with open(csv_path, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        return [row['username'] for row in reader if row['username'].strip()]

def fetch_user_info(username):
    params = {
        "name": username,
        "token": token
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
    with open(filename, "w", newline='', encoding='utf-8') as f:  # overwrite per user
        writer = csv.DictWriter(f, fieldnames=headers)
        writer.writeheader()
        for user in user_info:
            writer.writerow(user)

def main():
    input_csv = "threads_usersearch_input.csv"  # input file with 'username' column
    output_folder = "threads_usersearch_output"
    os.makedirs(output_folder, exist_ok=True)

    usernames = read_input_csv(input_csv)
    for username in usernames:
        print(f"Fetching user info for username: {username}")
        response_data = fetch_user_info(username)
        if response_data:
            formatted_data = format_user_data(response_data)
            output_csv = os.path.join(output_folder, f"threads_usersearch_{username}.csv")
            save_user_to_csv(formatted_data, output_csv)
            print(f"Saved user info for {username} to {output_csv}")

if __name__ == "__main__":
    main()
