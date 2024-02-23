import json
import os

from ..base_code.base_classes import AuraDBLoader


class AuraDBLoaderRikishiNodes(AuraDBLoader):
    def __init__(self):
        super().__init__()

    def create_rikishi_node(self, rikishi_data):
        with self.driver.session() as session:
            # Add a 'name' attribute that's equal to the 'id'
            rikishi_data["rikishiID"] = rikishi_data.pop("id")
            rikishi_data["name"] = rikishi_data["rikishiID"]

            # Cypher query to merge a rikishi node, preventing duplication
            # Cypher query to merge a rikishi node, preventing duplication
            query = (
                "MERGE (r:Rikishi {rikishiID: $rikishiID}) "
                "SET r += $attributes "
                "RETURN r"
            )
            session.run(
                query, rikishiID=rikishi_data["rikishiID"], attributes=rikishi_data
            )

    def load_jsons_and_create_rikishi_nodes(self, folder_path):
        for filename in os.listdir(folder_path):
            if filename.endswith(".json"):
                file_path = os.path.join(folder_path, filename)
                with open(file_path) as file:
                    rikishi_data = json.load(file)
                    self.create_rikishi_node(rikishi_data)
                print(f"Processed {filename}")


if __name__ == "__main__":
    loader = AuraDBLoaderRikishiNodes()
    try:
        # Get the most recent directory
        recent_dir = loader.get_most_recent_directory(loader.data_path)
        if recent_dir:
            rikishi_folder_path = os.path.join(loader.data_path, recent_dir, "rikishi")
            loader.load_jsons_and_create_rikishi_nodes(rikishi_folder_path)
        else:
            print("No recent directory found")
    finally:
        loader.close()
