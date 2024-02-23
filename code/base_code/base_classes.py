import json
import logging
import os
import re
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime

import requests  # type: ignore
from dotenv import load_dotenv
from neo4j import GraphDatabase
from requests.adapters import HTTPAdapter  # type: ignore


class SumoApiQuery:
    def __init__(self, iters=None, pool_size=20):
        self.log_file_name = "../../sumo_api_query_basho.log"
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
        if not os.path.exists(self.output_dir):
            os.makedirs(self.output_dir)
        logging.info(f"Making API call for: {iter_val}")
        try:
            response = self.session.get(url)
            # Check if the response is empty
            if not response.content.strip():
                logging.error("Empty response received")
                return "Empty response received"
            response.raise_for_status()
            response_data = response.json()
            # Check for specific error in response
            if response_data.get("error") == "INVALID_RIKISHI_ID":
                logging.error("Invalid rikishi id")
                return "Invalid rikishi id"  # Stop execution for this iteration
            # Save the file in the new directory
            with open(os.path.join(self.output_dir, f"{iter_val}.json"), "w") as file:
                json.dump(response_data, file)
            logging.info(f"API call successful for: {iter_val}")
        except requests.RequestException as e:
            logging.error(f"Error fetching data for {iter_val}: {e}")
        except Exception as e:
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


class AuraDBLoader:
    def __init__(self):
        load_dotenv()
        self.uri = os.environ.get("uri")
        self.user = os.environ.get("username")
        self.password = os.environ.get("password")
        self.data_path = "../../data"
        self.driver = GraphDatabase.driver(self.uri, auth=(self.user, self.password))

    def close(self):
        if self.driver:
            self.driver.close()

    def get_most_recent_directory(self, base_path):
        # Get all directories in the base path
        directories = [
            d
            for d in os.listdir(base_path)
            if os.path.isdir(os.path.join(base_path, d))
        ]
        # Filter directories by the YYYYMM pattern
        date_dirs = [d for d in directories if re.match(r"\d{6}", d)]
        # Sort directories by date, descending
        date_dirs.sort(key=lambda date: datetime.strptime(date, "%Y%m"), reverse=True)
        # Return the most recent directory, if available
        if date_dirs:
            return date_dirs[0]
        else:
            return None
