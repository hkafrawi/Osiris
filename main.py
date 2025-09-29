import requests
import pandas as pd
import json
from datetime import date, datetime
import configparser
import os
import logging
from typing import Callable
import httpx

# Load config
config = configparser.ConfigParser()
config.read("config.ini")
S_API_URL = config["API"]["url"]
C_API_URL = config["API"]["curl"]
SEOUDI_QUERY = config["QUERY"]["product"]
CARREFOUR_QUERY = config["QUERY"]["cproduct"]

# Configure logging
log_dir = "log_files"
os.makedirs(log_dir, exist_ok=True)

logging_file_name = f"log_{date.today().strftime('%m%d%Y')}.log"
log_file_path = os.path.join(log_dir, logging_file_name)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.FileHandler(log_file_path, mode="a"), logging.StreamHandler()],
)


def get_data_from_carrefour(item_id: str):
    logging.info(f"Fetching data for item ID: {item_id}")

    url = C_API_URL.format(item_id=item_id)

    payload = CARREFOUR_QUERY
    payload = json.loads(payload)
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
    
    # logging.info(payload)    
    try:
        with httpx.Client(http2=True, timeout=30.0) as client:
            r = client.post(url,headers=headers, json=payload)
        logging.info(r.status_code)
    except Exception as e:
        logging.error(e)
        r = {}
        pass

    return r


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
        logging.info(
            "json file's keys number {}".format(len(json_response["data"]["product"]))
        )

        result = pd.json_normalize(json_response["data"]["product"])
        return result
    except (KeyError, NotImplementedError):
        logging.error("Parsing for Carrefour Data")
        logging.info(
            "json file's keys number {}".format(
                len(json_response["data"]["placements"][0]["recommendedProducts"])
            )
        )

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
    for category, tag_groups in tags_data.items():
        for tag_group_name, tag_values in tag_groups.items():
            if tag_group_name == tag_name_input:
                compiled_tags.append((tag_values, tag_group_name, category))
    logging.info(f"compiled_tags for {tag_name_input}: {compiled_tags}")

    compiled_data = []
    for tags, tag_group, category in compiled_tags:
        for tag in tags:
            api_response = func(tag)
            structured_response = parse_data(api_response)
            structured_response["Category"] = category
            structured_response["Source"] = tag_group
            compiled_data.append(structured_response)
    logging.info(f"compiled_data: {len(compiled_data)}")

    df_data = pd.concat(compiled_data, ignore_index=True)
    df_data["Date"] = date.today().strftime("%m/%d/%Y")
    save_data_to_csv(df_data, tag_name_input)


def run():
    logging.info(
        "*** Run on {} ***".format(datetime.now().strftime("%m/%d/%Y %H:%M:%S"))
    )

    with open("required_data.json", "r") as json_file:
        required_data = json.load(json_file)

    today = date.today().strftime("%m%d%Y")

    seoudi_files = os.listdir("Seoudi_tags")
    carrefour_files = os.listdir("Carrefour_tags")

    soeudi_files = [x.split("_")[-1].split(".")[0] for x in seoudi_files]
    carrefour_files = [x.split("_")[-1].split(".")[0] for x in carrefour_files]

    if today not in soeudi_files:
        logging.info("Fetching new data for Seoudi_tags")
        fetsh_data(required_data, "Seoudi_tags", get_data_from_seoudi)
    else:
        logging.info("Seoudi_tags data is up to date.")

    if today not in carrefour_files:
        logging.info("Fetching new data for Carrefour_tags")
        fetsh_data(required_data, "Carrefour_tags", get_data_from_carrefour)
    else:
        logging.info("Carrefour_tags data is up to date.")


if __name__ == "__main__":
    run()
