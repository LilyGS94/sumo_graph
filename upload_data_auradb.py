import json
import os
import re
from datetime import datetime

from dotenv import load_dotenv
from neo4j import GraphDatabase

# Constants
data_path = "data"  # Path to the 'data' directory
# Load the .env file
load_dotenv()
# Accessing variables
password = os.environ.get("password")
uri = os.environ.get("uri")
username = os.environ.get("username")


class AuraDBLoader:
    def __init__(self, uri, user, password):
        self.driver = GraphDatabase.driver(uri, auth=(user, password))

    def close(self):
        if self.driver:
            self.driver.close()

    def create_node(self, basho_id):
        with self.driver.session() as session:
            # Cypher query to merge a node, preventing duplication
            query = "MERGE (b:Basho {bashoId: $basho_id}) " "RETURN b"
            result = session.run(query, basho_id=basho_id)
            return result.single()[0]

    def load_jsons_from_folder(self, folder_path):
        for filename in os.listdir(folder_path):
            if filename.endswith(".json"):
                file_path = os.path.join(folder_path, filename)
                with open(file_path) as file:
                    data = json.load(file)
                    basho_id = data.get(
                        "bashoId", ""
                    )  # Get bashoId or default to empty string
                    if basho_id:  # Check if basho_id is not empty
                        self.create_node(basho_id)
                        print(f"Processed and created node for {filename}")
                    else:
                        print(f"Skipped {filename} due to empty bashoId")

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


# Main execution
if __name__ == "__main__":
    loader = AuraDBLoader(uri, username, password)
    try:
        # Get the most recent directory
        recent_dir = loader.get_most_recent_directory(data_path)
        if recent_dir:
            basho_folder_path = os.path.join(data_path, recent_dir, "basho")
            loader.load_jsons_from_folder(basho_folder_path)
        else:
            print("No recent directory found")
    finally:
        loader.close()
