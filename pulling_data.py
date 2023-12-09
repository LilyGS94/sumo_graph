import requests
import logging
import json
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor

class SumoApiQuery:
    def __init__(self, timestamps):
        self.timestamps = timestamps
        self.base_url = 'https://www.sumo-api.com/api/basho/{}/banzuke/Makuuchi'
        self.results = {}
        self.session = requests.Session()

    def query_endpoint(self, timestamp):
        url = self.base_url.format(timestamp)
        try:
            response = self.session.get(url)
            if response.status_code == 200:
                self.results[timestamp] = response.json()
                logging.info(f'Successfully fetched data for {timestamp}')
            else:
                self.results[timestamp] = None
                logging.warning(f'Failed to fetch data for {timestamp}, status code: {response.status_code}')
        except requests.RequestException as e:
            logging.error(f'Error during request for {timestamp}: {e}')
            self.results[timestamp] = None

    def get_results(self):
        return self.results

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

def save_data_to_file(data, filename="sumo_data.json"):
    with open(filename, "w") as file:
        json.dump(data, file, indent=4)
    logging.info(f"Data successfully saved to {filename}")

if __name__ == "__main__":
    setup_logging()

    timestamps = generate_timestamps()
    print(f"Generated {len(timestamps)} timestamps to query.")

    sumo_api_query = SumoApiQuery(timestamps)

    print("Starting API queries...")
    with ThreadPoolExecutor(max_workers=10) as executor:  # Adjust max_workers as needed
        executor.map(sumo_api_query.query_endpoint, timestamps)

    data = sumo_api_query.get_results()

    print("Saving data to file...")
    save_data_to_file(data)

    print("Process completed.")

