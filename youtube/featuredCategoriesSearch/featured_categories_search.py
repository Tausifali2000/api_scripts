import requests
import csv

# Define the API endpoint and parameters
root = "https://ensembledata.com/apis"
endpoint = "/youtube/search/featured-categories"
params = {
    "name": "magic",  
    "token": "SfFWgfc5TFLgQmWy"  # Replace with your actual token
}

def fetch_featured_categories():
    res = requests.get(root + endpoint, params=params)
    if res.status_code != 200:
        print(f"Error fetching data: {res.status_code}")
        return None
    return res.json()

def format_data(data):
    categories = []
    for category in data.get("data", []):
        category_info = {
            "name": category.get("name", ""),
            "cursor": category.get("cursor", "")
        }
        categories.append(category_info)
    return categories

def save_to_csv(categories, filename):
    if not categories:
        print("No data to save.")
        return

    headers = categories[0].keys()
    with open(filename, "w", newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=headers)
        writer.writeheader()
        for category in categories:
            writer.writerow(category)

def main():
    response_data = fetch_featured_categories()
    if response_data:
        formatted_categories = format_data(response_data)
        save_to_csv(formatted_categories, "featured_categories.csv")
        print(f"Saved {len(formatted_categories)} categories to featured_categories.csv")

if __name__ == "__main__":
    main()
