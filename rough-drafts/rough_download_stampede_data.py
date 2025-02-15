import os
import requests
from urllib.parse import urljoin
from bs4 import BeautifulSoup
import time


def create_directory(path):
    if not os.path.exists(path):
        os.makedirs(path)


def download_file(url, local_path):
    try:
        response = requests.get(url, timeout=300)
        response.raise_for_status()

        with open(local_path, 'wb') as f:
            f.write(response.content)
        print(f"Successfully downloaded: {local_path}")

    except Exception as e:
        print(f"Error downloading {url}: {str(e)}")


def scrape_and_download(base_url, save_dir):
    # Create the main directory if it doesn't exist
    create_directory(save_dir)

    try:
        # Get the main page
        response = requests.get(base_url)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, 'html.parser')
        count = 0

        # Find all NODE directories
        for link in soup.find_all('a'):
            if count < 3:
                if 'NODE' in link.text:
                    node_url = urljoin(base_url, link['href'])
                    node_dir = os.path.join(save_dir, link.text.strip('/'))
                    create_directory(node_dir)

                    # Get the CSV files in each NODE directory
                    node_response = requests.get(node_url)
                    node_soup = BeautifulSoup(node_response.text, 'html.parser')
                    count += 1

                    for csv_link in node_soup.find_all('a'):
                        if csv_link.text.endswith('.csv'):
                            csv_url = urljoin(node_url, csv_link['href'])
                            csv_path = os.path.join(node_dir, csv_link.text)

                            # Download the CSV file
                            download_file(csv_url, csv_path)

                            # Add a small delay to avoid overwhelming the server
                            time.sleep(0.5)

                    print(f"Completed downloading files for {link.text}")

    except Exception as e:
        print(f"Error accessing {base_url}: {str(e)}")


if __name__ == "__main__":
    base_url = "https://www.datadepot.rcac.purdue.edu/sbagchi/fresco/repository/Stampede/TACC_Stats/"
    save_dir = r"---"

    scrape_and_download(base_url, save_dir)