import json
import os

import pandas as pd
from tqdm import tqdm

from base_classes import AuraDBLoader


class AuraDBLoaderBoutNodes(AuraDBLoader):
    def __init__(self):
        super().__init__()

    def create_bout_node(
        self,
        result_rikishi1,
        RikishiID_rikishi1,
        Side_rikishi1,
        kimarite,
        Fight_Number,
        result_rikishi2,
        RikishiID_rikishi2,
        Side_rikishi2,
        bashoId,
    ):
        with self.driver.session() as session:
            # Cypher query to merge a node, preventing duplication
            query = """MERGE (b:Bout {result_rikishi1: $result_rikishi1,rikishiId_rikishi1: $RikishiID_rikishi1,
                              side_rikishi1: $Side_rikishi1, fightNumber: $Fight_Number,
                               kimarite: $kimarite, result_rikishi2: $result_rikishi2,
                               rikishiId_rikishi2: $RikishiID_rikishi2,
                               side_rikishi2: $Side_rikishi2,bashoId:$bashoId})
                             RETURN b"""
            result = session.run(
                query,
                result_rikishi1=result_rikishi1,
                RikishiID_rikishi1=RikishiID_rikishi1,
                Side_rikishi1=Side_rikishi1,
                kimarite=kimarite,
                result_rikishi2=result_rikishi2,
                Fight_Number=Fight_Number,
                RikishiID_rikishi2=RikishiID_rikishi2,
                Side_rikishi2=Side_rikishi2,
                bashoId=bashoId,
            )
            return result.single()[0]

    def load_jsons_from_folder_and_create_bout_nodes(self, folder_path):
        for filename in tqdm(os.listdir(folder_path)):
            if filename.endswith(".json"):
                file_path = os.path.join(folder_path, filename)
                basho = filename.split(".json")[0]
                with open(file_path) as file:
                    data = json.load(file)
                    east_data = data["east"]
                    west_data = data["west"]
                    if east_data and west_data:
                        df_east = pd.DataFrame(east_data)
                        df_west = pd.DataFrame(west_data)
                        if "record" in df_east and "record" in df_west:
                            df_east_cleaned = df_east.dropna(subset=["record"])
                            df_east_cleaned.reset_index(drop=True, inplace=True)
                            df_west_cleaned = df_west.dropna(subset=["record"])
                            df_west_cleaned.reset_index(drop=True, inplace=True)
                            print(f"Processing and creating nodes for {basho}")
                            # Concatenating the 'record' columns from both DataFrames
                            east_records = df_east_cleaned["record"]
                            west_records = df_west_cleaned["record"]

                            # Flattening the list of records into a DataFrame
                            east_records = pd.json_normalize(east_records.sum())
                            west_records = pd.json_normalize(west_records.sum())

                            east_rikishi_ids = [
                                [rikishi_id] * len(record)
                                for rikishi_id, record in zip(
                                    df_east_cleaned["rikishiID"],
                                    df_east_cleaned["record"],
                                )
                            ]
                            west_rikishi_ids = [
                                [rikishi_id] * len(record)
                                for rikishi_id, record in zip(
                                    df_west_cleaned["rikishiID"],
                                    df_west_cleaned["record"],
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
                            matched_df = pd.merge(
                                all_records,
                                all_records,
                                left_on=["opponentID", "kimarite", "Fight_Number"],
                                right_on=["RikishiID", "kimarite", "Fight_Number"],
                                suffixes=("_rikishi1", "_rikishi2"),
                            )

                            # Assuming final_df is your DataFrame resulting from the merge operation
                            # Create a unique match identifier that is order-agnostic
                            matched_df["match_id"] = matched_df.apply(
                                lambda x: "_".join(
                                    sorted(
                                        [
                                            str(x["RikishiID_rikishi1"]),
                                            str(x["RikishiID_rikishi2"]),
                                        ]
                                    )
                                )
                                + "_"
                                + x["kimarite"]
                                + "_"
                                + str(x["Fight_Number"]),
                                axis=1,
                            )

                            unique_matches_df = matched_df.drop_duplicates(
                                subset=["match_id"]
                            )
                            left_merged_df = pd.merge(
                                all_records,
                                all_records,
                                left_on=["opponentID", "kimarite", "Fight_Number"],
                                right_on=["RikishiID", "kimarite", "Fight_Number"],
                                suffixes=("_rikishi1", "_rikishi2"),
                                how="left",
                                indicator=True,
                            )
                            no_match_df = left_merged_df[
                                left_merged_df["_merge"] == "left_only"
                            ]
                            unique_matches_df.rename(
                                columns={"bashoId_rikishi1": "bashoId"}, inplace=True
                            )
                            no_match_df.rename(
                                columns={"bashoId_rikishi1": "bashoId"}, inplace=True
                            )
                            #     for index, row in tqdm(all_records.iterrows()):
                            #         result = (
                            #             row["result"] if pd.notna(row.get("result", "")) else ""
                            #         )
                            #         opponentShikonaEn = (
                            #             row["opponentShikonaEn"]
                            #             if pd.notna(row.get("opponentShikonaEn", ""))
                            #             else ""
                            #         )
                            #         opponentID = (
                            #             row["opponentID"]
                            #             if pd.notna(row.get("opponentID", ""))
                            #             else ""
                            #         )
                            #         kimarite = (
                            #             row["kimarite"]
                            #             if pd.notna(row.get("kimarite", ""))
                            #             else ""
                            #         )
                            #         RikishiID = (
                            #             row["RikishiID"]
                            #             if pd.notna(row.get("RikishiID", ""))
                            #             else ""
                            #         )
                            #         Fight_Number = (
                            #             row["Fight_Number"]
                            #             if pd.notna(row.get("Fight_Number", 0))
                            #             else 0
                            #         )
                            #         Side = row["Side"] if pd.notna(row.get("Side", "")) else ""
                            #         bashoId = (
                            #             row["bashoId"]
                            #             if pd.notna(row.get("bashoId", ""))
                            #             else ""
                            #         )
                            #         self.create_bout_node(
                            #             result=result,
                            #             opponentShikonaEn=opponentShikonaEn,
                            #             opponentID=opponentID,
                            #             kimarite=kimarite,
                            #             RikishiID=RikishiID,
                            #             Fight_Number=Fight_Number,
                            #             Side=Side,
                            #             basho=bashoId,
                            #         )
                            # else:
                            #     print(f"Skipped {filename} because of missing data")
                            #     continue
                            for index, row in unique_matches_df.iterrows():
                                result_rikishi1 = (
                                    row["result_rikishi1"]
                                    if pd.notna(row.get("result_rikishi1", ""))
                                    else ""
                                )
                                RikishiID_rikishi1 = (
                                    row["RikishiID_rikishi1"]
                                    if pd.notna(row.get("RikishiID_rikishi1", ""))
                                    else ""
                                )
                                Side_rikishi1 = (
                                    row["Side_rikishi1"]
                                    if pd.notna(row.get("Side_rikishi1", ""))
                                    else ""
                                )
                                kimarite = (
                                    row["kimarite"]
                                    if pd.notna(row.get("kimarite", ""))
                                    else ""
                                )

                                Fight_Number = (
                                    row["Fight_Number"]
                                    if pd.notna(row.get("Fight_Number", 0))
                                    else 0
                                )
                                result_rikishi2 = (
                                    row["result_rikishi2"]
                                    if pd.notna(row.get("result_rikishi2", ""))
                                    else ""
                                )
                                RikishiID_rikishi2 = (
                                    row["RikishiID_rikishi2"]
                                    if pd.notna(row.get("RikishiID_rikishi2", ""))
                                    else ""
                                )
                                Side_rikishi2 = (
                                    row["Side_rikishi2"]
                                    if pd.notna(row.get("Side_rikishi2", ""))
                                    else ""
                                )
                                bashoId = (
                                    row["bashoId"]
                                    if pd.notna(row.get("bashoId", ""))
                                    else ""
                                )
                                self.create_bout_node(
                                    result_rikishi1=result_rikishi1,
                                    RikishiID_rikishi1=RikishiID_rikishi1,
                                    Side_rikishi1=Side_rikishi1,
                                    kimarite=kimarite,
                                    result_rikishi2=result_rikishi2,
                                    Fight_Number=Fight_Number,
                                    RikishiID_rikishi2=RikishiID_rikishi2,
                                    Side_rikishi2=Side_rikishi2,
                                    bashoId=bashoId,
                                )
                            for index, row in no_match_df.iterrows():
                                result_rikishi1 = (
                                    row["result_rikishi1"]
                                    if pd.notna(row.get("result_rikishi1", ""))
                                    else ""
                                )
                                RikishiID_rikishi1 = (
                                    row["RikishiID_rikishi1"]
                                    if pd.notna(row.get("RikishiID_rikishi1", ""))
                                    else ""
                                )
                                Side_rikishi1 = (
                                    row["Side_rikishi1"]
                                    if pd.notna(row.get("Side_rikishi1", ""))
                                    else ""
                                )
                                kimarite = (
                                    row["kimarite"]
                                    if pd.notna(row.get("kimarite", ""))
                                    else ""
                                )

                                Fight_Number = (
                                    row["Fight_Number"]
                                    if pd.notna(row.get("Fight_Number", 0))
                                    else 0
                                )
                                result_rikishi2 = (
                                    row["result_rikishi2"]
                                    if pd.notna(row.get("result_rikishi2", ""))
                                    else ""
                                )
                                RikishiID_rikishi2 = (
                                    row["RikishiID_rikishi2"]
                                    if pd.notna(row.get("RikishiID_rikishi2", ""))
                                    else ""
                                )
                                Side_rikishi2 = (
                                    row["Side_rikishi2"]
                                    if pd.notna(row.get("Side_rikishi2", ""))
                                    else ""
                                )
                                bashoId = (
                                    row["bashoId"]
                                    if pd.notna(row.get("bashoId", ""))
                                    else ""
                                )
                                self.create_bout_node(
                                    result_rikishi1=result_rikishi1,
                                    RikishiID_rikishi1=RikishiID_rikishi1,
                                    Side_rikishi1=Side_rikishi1,
                                    kimarite=kimarite,
                                    result_rikishi2=result_rikishi2,
                                    Fight_Number=Fight_Number,
                                    RikishiID_rikishi2=RikishiID_rikishi2,
                                    Side_rikishi2=Side_rikishi2,
                                    bashoId=bashoId,
                                )
                        else:
                            print(f"Skipped {filename} because of missing data")
                            continue
                    else:
                        print(f"Skipped {filename} because of missing data")
                        continue


# Main execution
if __name__ == "__main__":
    loader = AuraDBLoaderBoutNodes()
    try:
        # Get the most recent directory
        recent_dir = loader.get_most_recent_directory(loader.data_path)
        if recent_dir:
            basho_folder_path = os.path.join(loader.data_path, recent_dir, "basho")
            loader.load_jsons_from_folder_and_create_bout_nodes(basho_folder_path)
        else:
            print("No recent directory found")
    finally:
        loader.close()
