import json
import os
import re
from datetime import datetime

import pandas as pd
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

    def create_bout_node(
        self,
        result,
        opponentShikonaEn,
        opponentID,
        kimarite,
        RikishiID,
        Fight_Number,
        Side,
        basho,
    ):
        with self.driver.session() as session:
            # Cypher query to merge a node, preventing duplication
            query = """MERGE (b:Bout {result: $result,rikishiID: $RikishiID,rikishiSide: $Side, fightNumber: $Fight_Number,
                   kimarite: $kimarite, opponentID: $opponentID, opponentShikonaEn: $opponentShikonaEn, basho:$basho})
                 RETURN b"""  # TODO: Change to create bout node
            result = session.run(
                query,
                result=result,
                opponentShikonaEn=opponentShikonaEn,
                opponentID=opponentID,
                kimarite=kimarite,
                RikishiID=RikishiID,
                Fight_Number=Fight_Number,
                Side=Side,
                basho=basho,
            )
            return result.single()[0]

    def load_jsons_from_folder(self, folder_path):
        for filename in os.listdir(folder_path):
            if filename.endswith(".json"):
                file_path = os.path.join(folder_path, filename)
                basho = filename.split(".json")[
                    0
                ]  # TODO: Use this to create relationship between newly created bout node and basho
                with open(file_path) as file:
                    data = json.load(file)
                    east_data = data["east"]
                    west_data = data["west"]
                    if east_data and west_data:
                        print(f"Processing and creating nodes for {basho}")
                        df_east = pd.DataFrame(east_data)
                        df_east_cleaned = df_east.dropna(subset=["record"])
                        df_east_cleaned.reset_index(drop=True, inplace=True)
                        df_west = pd.DataFrame(west_data)
                        df_west_cleaned = df_west.dropna(subset=["record"])
                        df_west_cleaned.reset_index(drop=True, inplace=True)
                        # Concatenating the 'record' columns from both DataFrames
                        east_records = df_east_cleaned["record"]
                        west_records = df_west_cleaned["record"]

                        # Flattening the list of records into a DataFrame
                        east_records = pd.json_normalize(east_records.sum())
                        west_records = pd.json_normalize(west_records.sum())

                        east_rikishi_ids = [
                            [rikishi_id] * len(record)
                            for rikishi_id, record in zip(
                                df_east_cleaned["rikishiID"], df_east_cleaned["record"]
                            )
                        ]
                        west_rikishi_ids = [
                            [rikishi_id] * len(record)
                            for rikishi_id, record in zip(
                                df_west_cleaned["rikishiID"], df_west_cleaned["record"]
                            )
                        ]
                        flat_east_rikishi_ids = [
                            id for sublist in east_rikishi_ids for id in sublist
                        ]
                        flat_west_rikishi_ids = [
                            id for sublist in west_rikishi_ids for id in sublist
                        ]
                        east_records["RikishiID"] = flat_east_rikishi_ids
                        west_records["RikishiID"] = flat_west_rikishi_ids

                        west_records["Fight_Number"] = (
                            west_records.groupby("RikishiID").cumcount() + 1
                        )
                        east_records["Fight_Number"] = (
                            east_records.groupby("RikishiID").cumcount() + 1
                        )
                        east_records["Side"] = "East"
                        west_records["Side"] = "West"
                        all_records = pd.concat([east_records, west_records])
                        all_records["bashoId"] = basho

                        for index, row in all_records.iterrows():
                            result = (
                                row["result"] if pd.notna(row.get("result", "")) else ""
                            )
                            opponentShikonaEn = (
                                row["opponentShikonaEn"]
                                if pd.notna(row.get("opponentShikonaEn", ""))
                                else ""
                            )
                            opponentID = (
                                row["opponentID"]
                                if pd.notna(row.get("opponentID", ""))
                                else ""
                            )
                            kimarite = (
                                row["kimarite"]
                                if pd.notna(row.get("kimarite", ""))
                                else ""
                            )
                            RikishiID = (
                                row["RikishiID"]
                                if pd.notna(row.get("RikishiID", ""))
                                else ""
                            )
                            Fight_Number = (
                                row["Fight_Number"]
                                if pd.notna(row.get("Fight_Number", 0))
                                else 0
                            )
                            Side = row["Side"] if pd.notna(row.get("Side", "")) else ""
                            bashoId = (
                                row["bashoId"]
                                if pd.notna(row.get("bashoId", ""))
                                else ""
                            )
                            print(f"creating node for bout {row}")
                            self.create_bout_node(
                                result=result,
                                opponentShikonaEn=opponentShikonaEn,
                                opponentID=opponentID,
                                kimarite=kimarite,
                                RikishiID=RikishiID,
                                Fight_Number=Fight_Number,
                                Side=Side,
                                basho=bashoId,
                            )
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
