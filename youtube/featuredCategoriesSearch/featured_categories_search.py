import requests
import csv
import os
import pandas as pd

# Define the API root URL
root = "https://ensembledata.com/apis"

def read_input_csv(csv_path):
    """Read input CSV file and return a list of parameters."""
    df = pd.read_csv(csv_path, dtype=str)
    return df['hashtag'].dropna().tolist() 
def fetch_featured_categories(name, token):
    """Fetch featured categories from the API."""
    endpoint = "/youtube/search/featured-categories"
    params = {
        "name": name,
        "token": token
    }
    res = requests.get(root + endpoint, params=params)
    if res.status_code != 200:
        print(f"Error fetching data for '{name}': {res.status_code} - {res.text}")
        return None
    return res.json()

def format_category_data(data):
    """Format the fetched category data into a structured format."""
    category_info_list = []
    for category in data.get("data", []):
        category_info = {
            "name": category.get("name", ""),
            "cursor": category.get("cursor", "")
        }
        category_info_list.append(category_info)
    return category_info_list

def save_data_to_csv(data_list, filename):
    """Save the formatted data to a CSV file."""
    if not data_list:
        print("No data to save.")
        return

    headers = ["name", "cursor"]
    with open(filename, "w", newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(headers)
        for data in data_list:
            writer.writerow([data["name"], data["cursor"]])

def main():
    input_csv = "input.csv" 
    token = "B3jQjPFTs8Y88lXj"  
    output_folder = "output_data"
    os.makedirs(output_folder, exist_ok=True)  
    output_file = os.path.join(output_folder, "featured_categories_data.csv")

    parameters = read_input_csv(input_csv)
    all_category_info = []
    for param in parameters:
        response_data = fetch_featured_categories(param, token)
        if response_data:
            formatted_data = format_category_data(response_data)
            all_category_info.extend(formatted_data)

    save_data_to_csv(all_category_info, output_file)
    print(f"Saved category data to {output_file}")

if __name__ == "__main__":
    main()
