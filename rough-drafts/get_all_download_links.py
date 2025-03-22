import os
from urllib.parse import urljoin
import requests
from bs4 import BeautifulSoup
import json


def download_folder_threaded(folder_url: str, local_folder: str, required_files, headers: dict):
    """
    Scrapes a directory listing for nodes, then extracts and returns the links to required CSV files within each node.

    Args:
        folder_url (str): Base URL containing NODE directories.
        local_folder (str): Local directory for saving files (not used here but kept for future use).
        required_files (list): List of required CSV filenames to look for in each node.
        headers (dict): HTTP headers for the request.

    Returns:
        dict: A dictionary where keys are NODE names and values are lists of URLs to required files.
    """
    response = requests.get(folder_url, headers=headers, timeout=30)
    response.raise_for_status()
    soup = BeautifulSoup(response.text, 'html.parser')

    node_dict = {}

    # Iterate over all NODE directories
    for link in soup.find_all('a'):
        node_name = link.text.strip().rstrip('/')

        # Skip if it's not a NODE directory
        if not node_name.startswith("NODE"):
            continue

        node_url = urljoin(folder_url, link['href'])

        # Fetch NODE page
        node_response = requests.get(node_url, headers=headers, timeout=30)
        node_response.raise_for_status()
        node_soup = BeautifulSoup(node_response.text, 'html.parser')

        # Find required files in NODE
        file_links = []
        for file_link in node_soup.find_all('a'):
            if file_link.text.strip() in required_files:
                file_url = urljoin(node_url, file_link['href'])
                file_links.append(file_url)

        if file_links:
            print(f"Added {node_name}")
            node_dict[node_name] = file_links

        with open("node-list.json", "w") as json_file:
            json.dump(node_dict, json_file, indent=4)

    return node_dict


def update_json_with_complete_flag(json_file_path, output_file_path=None):
    """
    Update a JSON file by adding a "Complete" flag set to False for each NODE key.

    Parameters:
    json_file_path (str): Path to the input JSON file
    output_file_path (str, optional): Path to save the updated JSON. If None, overwrites the input file.

    Returns:
    bool: True if successful, False otherwise
    """
    try:
        # Read the original JSON file
        with open(json_file_path, 'r') as file:
            data = json.load(file)

        # Create a new dictionary with the updated structure
        updated_data = {}

        # Process each NODE key
        for node_key, urls in data.items():
            updated_data[node_key] = {
                "urls": urls,
                "Complete": False
            }

        # Determine where to save the updated JSON
        save_path = output_file_path if output_file_path else json_file_path

        # Write the updated JSON to file
        with open(save_path, 'w') as file:
            json.dump(updated_data, file, indent=4)

        return True

    except Exception as e:
        print(f"Error updating JSON file: {str(e)}")
        return False


if __name__ == "__main__":
    # Replace with your actual file path
    file_path = "node-list.json"
    result = update_json_with_complete_flag(file_path)

    if result:
        print(f"Successfully updated {file_path}")
    else:
        print("Failed to update the JSON file")
