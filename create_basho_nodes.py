import json
import os

from base_classes import AuraDBLoader


class AuraDBLoaderBashoNodes(AuraDBLoader):
    def __init__(self):
        super().__init__()

    def create_basho_node(self, basho_id):
        with self.driver.session() as session:
            # Cypher query to merge a node, preventing duplication
            query = "MERGE (b:Basho {bashoId: $basho_id}) " "RETURN b"
            result = session.run(query, basho_id=basho_id)
            return result.single()[0]

    def load_jsons_from_folder_and_create_basho_nodes(self, folder_path):
        for filename in os.listdir(folder_path):
            if filename.endswith(".json"):
                file_path = os.path.join(folder_path, filename)
                with open(file_path) as file:
                    data = json.load(file)
                    basho_id = data.get(
                        "bashoId", ""
                    )  # Get bashoId or default to empty string
                    if basho_id:  # Check if basho_id is not empty
                        self.create_basho_node(basho_id)
                        print(f"Processed and created node for {filename}")
                    else:
                        print(f"Skipped {filename} due to empty bashoId")


# Main execution
if __name__ == "__main__":
    loader = AuraDBLoaderBashoNodes()
    try:
        # Get the most recent directory
        recent_dir = loader.get_most_recent_directory(loader.data_path)
        if recent_dir:
            basho_folder_path = os.path.join(loader.data_path, recent_dir, "basho")
            loader.load_jsons_from_folder_and_create_basho_nodes(basho_folder_path)
        else:
            print("No recent directory found")
    finally:
        loader.close()
