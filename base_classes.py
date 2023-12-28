import json
import logging
import os
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime

import requests  # type: ignore
from requests.adapters import HTTPAdapter  # type: ignore


class SumoApiQuery:
    def __init__(self, iters=None, pool_size=20):
        self.log_file_name = "sumo_api_query_basho.log"
        self.iters = iters
        self.base_url = "https://www.sumo-api.com/api/basho/{}/banzuke/Makuuchi"
        self.session = requests.Session()
        adapter = HTTPAdapter(pool_connections=pool_size, pool_maxsize=pool_size)
        self.session.mount("https://", adapter)
        self.session.mount("http://", adapter)
        # Create a directory named with the current timestamp
        self.now = datetime.now().strftime("%Y%m")
        self.output_dir = f"data/{self.now}/basho"
        self.base_directory = "data/"

    def query_endpoint(self, iter_val):
        url = self.base_url.format(iter_val)
        print(url)
        logging.info(f"Making API call for: {iter_val}")
        try:
            response = self.session.get(url)
            response.raise_for_status()
            response_data = response.json()
            # Save the file in the new directory
            with open(os.path.join(self.output_dir, f"{iter_val}.json"), "w") as file:
                json.dump(response_data, file)
            logging.info(f"API call successful for: {iter_val}")
        except requests.RequestException as e:
            logging.error(f"Error fetching data for {iter_val}: {e}")

    def setup_logging(self):
        logging.basicConfig(
            level=logging.INFO,
            format="%(asctime)s - %(levelname)s - %(message)s",
            filename=self.log_file_name,
            filemode="w",
        )

    def run_queries(self):
        self.setup_logging()
        if not os.path.exists(self.output_dir):
            os.makedirs(self.output_dir)
        with ThreadPoolExecutor() as executor:
            executor.map(self.query_endpoint, self.iters)
