import requests
import pandas as pd
import json
from datetime import date
import configparser

# Load config
config = configparser.ConfigParser()
config.read("config.ini")

API_URL = config["API"]["url"]
PRODUCT_QUERY = config["QUERY"]["product"]


def get_data_from_seoudi(item_id: str):
    url = API_URL

    payload = json.dumps({"query": PRODUCT_QUERY, "variables": {"slug": f"{item_id}"}})
    headers = {
        "Content-Type": "application/json",
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:142.0)"
            " Gecko/20100101 Firefox/142.0"
        ),
        "store": "default",
    }

    response = requests.request("POST", url, headers=headers, data=payload)
    return response


def parse_data(response):
    json_response = json.loads(response.text)
    return pd.json_normalize(json_response["data"]["product"])


required_data = [
    "al-doha-sugar-1-kg",
    "afia-sunflower-oil-700-ml",
    "crystal-sunflower-oil-700-ml",
    "al-doha-rice-1-kg",
    "mafa-baladi-tomato-1-kg",
    "chicken-breasts",
    "chicken-thigh",
    "baladi-beef-kebab-halla",
    "seoudi-baladi-eggs-10-pieces",
    "al-waha-white-eggs-10-pieces",
]

data = []
for item in required_data:
    api_response = get_data_from_seoudi(item)
    structured_response = parse_data(api_response)
    data.append(structured_response)

try:
    final_df = pd.concat(data, ignore_index=True)
    final_df["Date"] = date.today().strftime("%m/%d/%Y")
    final_df["Source"] = "Seoudi"
    final_df.to_csv("data.csv", index=False)
except ValueError:
    print(data)
