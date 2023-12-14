import os
import json

class RikishiIDExtractor:
    def __init__(self, base_directory):
        self.base_directory = base_directory

    def get_latest_directory(self):
        # List all directories in the base directory
        all_dirs = [os.path.join(self.base_directory, d) for d in os.listdir(self.base_directory) 
                    if os.path.isdir(os.path.join(self.base_directory, d))]

        if not all_dirs:
            return None

        # Sort directories by their timestamp
        latest_dir = max(all_dirs, key=os.path.getmtime)
        return latest_dir

    def extract_rikishi_ids_from_directory(self, directory):
        unique_rikishi_ids = set()

        # List all JSON files in the directory
        json_files = [f for f in os.listdir(directory) if f.endswith('.json')]

        if not json_files:
            return unique_rikishi_ids

        for file_name in json_files:
            file_path = os.path.join(directory, file_name)
            try:
                with open(file_path, 'r') as file:
                    data = json.load(file)
            except json.JSONDecodeError:
                continue

            # Check if 'east' and 'west' are not None before processing
            east_rikishi_ids = [rikishi.get('rikishiID') for rikishi in data.get('east', []) if rikishi] if data.get('east') else []
            west_rikishi_ids = [rikishi.get('rikishiID') for rikishi in data.get('west', []) if rikishi] if data.get('west') else []

            # Combine and deduplicate the list, removing any None values
            unique_rikishi_ids.update([id for id in east_rikishi_ids if id is not None])
            unique_rikishi_ids.update([id for id in west_rikishi_ids if id is not None])

        return list(unique_rikishi_ids)

    def process_latest_directory(self):
        latest_dir = self.get_latest_directory()
        if latest_dir:
            return self.extract_rikishi_ids_from_directory(latest_dir)
        else:
            return "No directories found."

# Usage example
base_directory = 'data/'  # Replace with your base directory path
extractor = RikishiIDExtractor(base_directory)
unique_ids = extractor.process_latest_directory()
print(unique_ids)
