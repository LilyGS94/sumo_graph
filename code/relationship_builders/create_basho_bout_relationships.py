import json
import os

from ..base_code.base_classes import AuraDBLoader


class AuraDBLoaderBashoBoutRelationships(AuraDBLoader):
    def __init__(self):
        super().__init__()

    def create_basho_bout_relationship(self, bashoId):
        with self.driver.session() as session:
            query = """
            MATCH (b:Basho), (b2:Bout)
            WHERE b.bashoId = $bashoId AND b2.bashoId = $bashoId
            MERGE (b)-[:BOUT_EVENT]->(b2)
            """
            result = session.run(query, bashoId=bashoId)
            record = result.single()
            if record is None:
                return None  # Or handle this case as you see fit
            return record[0]

    def run_create_basho_bout_relationship(self, folder_path):
        for filename in os.listdir(folder_path):
            if filename.endswith(".json"):
                file_path = os.path.join(folder_path, filename)
                with open(file_path) as file:
                    data = json.load(file)
                    basho_id = data.get(
                        "bashoId", ""
                    )  # Get bashoId or default to empty string
                    if basho_id:
                        print(basho_id)  # Check if basho_id is not empty
                        self.create_basho_bout_relationship(bashoId=basho_id)
                        print(f"Processed and created relationships for {basho_id}")
                    else:
                        print(f"Skipped {filename} due to empty bashoId")


if __name__ == "__main__":
    loader = AuraDBLoaderBashoBoutRelationships()
    try:
        # Get the most recent directory
        recent_dir = loader.get_most_recent_directory(loader.data_path)
        if recent_dir:
            basho_folder_path = os.path.join(loader.data_path, recent_dir, "basho")
            loader.run_create_basho_bout_relationship(basho_folder_path)
        else:
            print("No recent directory found")
    finally:
        loader.close()
