import os

import requests
import logging
import json
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor

from requests.adapters import HTTPAdapter


class SumoApiQuery:
    def __init__(self, timestamps, pool_size=20):
        self.timestamps = timestamps
        self.base_url = 'https://www.sumo-api.com/api/basho/{}/banzuke/Makuuchi'
        self.session = requests.Session()
        adapter = HTTPAdapter(pool_connections=pool_size, pool_maxsize=pool_size)
        self.session.mount('https://', adapter)
        self.session.mount('http://', adapter)
        # Create a directory named with the current timestamp
        self.now = datetime.now().strftime("%Y%m")
        self.output_dir = f"data/{self.now}"
        if not os.path.exists(self.output_dir):
            os.makedirs(self.output_dir)

    def query_endpoint(self, timestamp):
        url = self.base_url.format(timestamp)
        logging.info(f"Making API call for timestamp: {timestamp}")
        try:
            response = self.session.get(url)
            response.raise_for_status()
            response_data = response.json()
            # Save the file in the new directory
            with open(os.path.join(self.output_dir, f'{timestamp}.json'), 'w') as file:
                json.dump(response_data, file)
            logging.info(f"API call successful for timestamp: {timestamp}")
        except requests.RequestException as e:
            logging.error(f"Error fetching data for {timestamp}: {e}")

    def run_queries(self):
        with ThreadPoolExecutor() as executor:
            executor.map(self.query_endpoint, self.timestamps)


def generate_timestamps():
    current_year = datetime.now().year
    current_month = datetime.now().month

    timestamps = []
    for year in range(1958, current_year + 1):
        for month in ['01', '03', '05', '07', '09', '11']:
            if year == current_year and int(month) > current_month:
                break
            timestamps.append(f'{year}{month}')
    return timestamps

def setup_logging():
    logging.basicConfig(level=logging.INFO,
                        format='%(asctime)s - %(levelname)s - %(message)s',
                        filename='sumo_api_query.log',
                        filemode='w')

# def save_data_to_file(data, filename="sumo_data.json"):
#     with open(filename, "w") as file:
#         json.dump(data, file, indent=4)
#     logging.info(f"Data successfully saved to {filename}")

if __name__ == "__main__":
    setup_logging()
    timestamps = generate_timestamps()
    print(f"Generated {len(timestamps)} timestamps to query.")

    query = SumoApiQuery(timestamps)
    query.run_queries()

    #sumo_api_query = SumoApiQuery(timestamps)

    # print("Starting API queries...")
    # with ThreadPoolExecutor(max_workers=10) as executor:  # Adjust max_workers as needed
    #     executor.map(sumo_api_query.query_endpoint, timestamps)
    #
    # data = sumo_api_query.get_results()

    #print("Saving data to file...")
    #save_data_to_file(data)

    print("Process completed.")

