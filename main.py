import requests
import pandas as pd
import json
from datetime import date
import configparser
import os
import logging
from typing import Callable

# Load config
config = configparser.ConfigParser()
config.read("config.ini")
S_API_URL = config["API"]["url"]
C_API_URL = config["API"]["curl"]
SEOUDI_QUERY = config["QUERY"]["product"]
CARREFOUR_QUERY = config["QUERY"]["cproduct"]

# Configure logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)


def get_data_from_carrefour(item_id: str):
    logging.info(f"Fetching data for item ID: {item_id}")

    url = C_API_URL.format(item_id=item_id)

    payload = CARREFOUR_QUERY
    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:142.0)"
            " Gecko/20100101 Firefox/142.0"
        ),
        "Accept": "application/json",
        "Accept-Language": "en-US,en;q=0.5",
        "Accept-Encoding": "gzip, deflate, br, zstd",
        "content-type": "application/json; charset=utf-8",
        "storeid": "mafegy",
        "Origin": "https://www.carrefouregypt.com",
        "Connection": "keep-alive",
    }

    response = requests.request("POST", url, headers=headers, data=payload)

    logging.info(response.status_code)
    return response


def get_data_from_seoudi(item_id: str):
    logging.info(f"Fetching data for item ID: {item_id}")
    url = S_API_URL

    payload = json.dumps({"query": SEOUDI_QUERY, "variables": {"slug": f"{item_id}"}})
    headers = {
        "Content-Type": "application/json",
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:142.0)"
            " Gecko/20100101 Firefox/142.0"
        ),
        "store": "default",
    }

    response = requests.request("POST", url, headers=headers, data=payload)
    logging.info(response.status_code)
    return response


def parse_data(response):
    json_response = json.loads(response.text)
    try:
        logging.info("Parsing for Seoudi Data")
        logging.info(json_response["data"]["product"])

        result = pd.json_normalize(json_response["data"]["product"])
        return result
    except (KeyError, NotImplementedError):
        logging.error("Parsing for Carrefour Data")
        logging.info(json_response["data"]["placements"][0]["recommendedProducts"])

        result = pd.json_normalize(
            json_response["data"]["placements"][0]["recommendedProducts"]
        )
        return result


def save_data_to_csv(data: pd.DataFrame, source: str):
    today = date.today().strftime("%m%d%Y")
    filename = f"{source}_{today}.csv"
    os.makedirs(source, exist_ok=True)
    filepath = os.path.join(source, filename)
    data.to_csv(filepath, index=False)


def fetsh_data(tags_data: json, tag_name_input: str, func: Callable) -> None:
    # arange tags for fetching
    logging.info(f"Fetching data for tag: {tag_name_input}")
    logging.info(f"tags_data: {tags_data}")
    compiled_tags = []

    for category in tags_data.keys():
        for tag_name, tag_value in tags_data.get(category).items():
            if tag_name == tag_name_input:
                compiled_tags.extend(tag_value)
    logging.info(f"compiled_tags for {tag_name_input}: {compiled_tags}")

    compiled_data = []
    for item in compiled_tags:
        api_response = func(item)
        structured_response = parse_data(api_response)
        compiled_data.append(structured_response)
    logging.info(f"compiled_data: {compiled_data}")

    df_data = pd.concat(compiled_data, ignore_index=True)
    df_data["Date"] = date.today().strftime("%m/%d/%Y")
    df_data["Source"] = tag_name
    save_data_to_csv(df_data, tag_name_input)


if __name__ == "__main__":
    with open("required_data.json", "r") as json_file:
        required_data = json.load(json_file)

    fetsh_data(required_data, "Seoudi_tags", get_data_from_seoudi)
    fetsh_data(required_data, "Carrefour_tags", get_data_from_carrefour)
