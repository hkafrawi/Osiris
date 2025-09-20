"""
A script compiled and ran on 20.09.2025 to include
Category from required_json file to previously obtained data.

"""

import pandas as pd
import json
import os


def return_category(tag: str):
    if type(tag) is not str:
        tag = str(tag)
    with open("required_data.json", "r") as json_file:
        data_file = json.load(json_file)
    for category, tag_groups in data_file.items():
        for tag_group_name, tag_values in tag_groups.items():
            if tag in tag_values:
                return category


def read_datafiles(directory, map_key, func):
    for file in os.listdir(directory):
        date_stamp = file.split("_")[-1].split(".")[0]
        file_path = os.path.join(directory, file)
        df = pd.read_csv(file_path)
        df["Category"] = df[map_key].map(func)
        save_data_to_csv(df, directory, date_stamp)


def save_data_to_csv(data: pd.DataFrame, source: str, date_stamp: str):
    filename = f"{source}_{date_stamp}.csv"
    os.makedirs(source, exist_ok=True)
    filepath = os.path.join(source, filename)
    data.to_csv(filepath, index=False)


directory = "Carrefour_tags"
read_datafiles(directory, "id", return_category)
