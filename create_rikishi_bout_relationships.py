import json
import os

from base_classes import AuraDBLoader


class AuraDBLoaderRikishiBoutRelationships(AuraDBLoader):
    def __init__(self):
        super().__init__()

    def create_rikishi_bout_relationship(self, rikishiId):
        with self.driver.session() as session:
            query = """MATCH (r:Rikishi)
                        WHERE r.rikishiID = $rikishiId
                        WITH r
                        MATCH (b:Bout)
                        WHERE b.rikishiId_rikishi1 = r.rikishiID OR b.rikishiId_rikishi2 = r.rikishiID
                        MERGE (r)-[:RIKISHI_IN_BOUT_EVENT]->(b)"""
            result = session.run(query, rikishiId=rikishiId)
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
                    rikishi_id = data.get("id", "")
                    if rikishi_id:
                        print(rikishi_id)
                        self.create_rikishi_bout_relationship(rikishiId=rikishi_id)
                        print(f"Processed and created relationships for {rikishi_id}")
                    else:
                        print(f"Skipped {filename} due to empty rikishiId")


# Main execution
if __name__ == "__main__":
    loader = AuraDBLoaderRikishiBoutRelationships()
    try:
        # Get the most recent directory
        recent_dir = loader.get_most_recent_directory(loader.data_path)
        if recent_dir:
            basho_folder_path = os.path.join(loader.data_path, recent_dir, "rikishi")
            loader.run_create_basho_bout_relationship(basho_folder_path)
        else:
            print("No recent directory found")
    finally:
        loader.close()
