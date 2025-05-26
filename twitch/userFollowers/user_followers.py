import requests
import csv

# Define the API endpoint and parameters
root = "https://ensembledata.com/apis"
endpoint = "/twitch/user/followers"
params = {
    "username": "hmatttv", 
    "token": "SfFWgfc5TFLgQmWy"  # Replace with your actual token
}

def fetch_followers():
    res = requests.get(root + endpoint, params=params)
    if res.status_code != 200:
        print(f"Error fetching data: {res.status_code}")
        return None
    return res.json()

def save_to_csv(followers_count, filename):
    with open(filename, "w", newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(["username", "followers_count"])
        writer.writerow([params["username"], followers_count])

def main():
    response_data = fetch_followers()
    if response_data:
        followers_count = response_data.get("data", 0)
        save_to_csv(followers_count, "user_followers.csv")
        print(f"Saved followers count for {params['username']} to user_followers.csv")

if __name__ == "__main__":
    main()
