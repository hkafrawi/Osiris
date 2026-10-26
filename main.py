import requests
import pandas as pd
import json
from datetime import date, datetime
import configparser
import os
import logging
from typing import Callable
import httpx
import random
import time

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
    payload = json.loads(CARREFOUR_QUERY)
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
        "Cookie": "cart_api=v2; cart_api=v2",
    }

    def _send():
        with httpx.Client(http2=True, timeout=30.0) as client:
            return client.post(url, headers=headers, json=payload)

    r = post_with_backoff(_send, CARREFOUR_LIMITER)
    logging.info(r.status_code)
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

    response = requests.request("POST", url, headers=headers, data=payload, timeout=30)
    logging.info(response.status_code)
    return response

class RateLimiter:
    """Enforce a minimum interval between requests."""
    def __init__(self, min_interval: float, jitter: float = 0.3):
        self.min_interval = min_interval
        self.jitter = jitter
        self._last = 0.0

    def wait(self) -> None:
        target = self.min_interval + random.uniform(0, self.jitter)
        remaining = target - (time.monotonic() - self._last)
        if remaining > 0:
            time.sleep(remaining)
        self._last = time.monotonic()


CARREFOUR_LIMITER = RateLimiter(min_interval=1.5)


def post_with_backoff(send, limiter, max_attempts=5, base_delay=10.0, max_wait=120.0):
    """Throttle, send, and retry 429/5xx with backoff. Returns the final response."""
    for attempt in range(1, max_attempts + 1):
        limiter.wait()
        response = send()
        if response.status_code not in (429, 500, 502, 503, 504) or attempt == max_attempts:
            return response

        retry_after = response.headers.get("Retry-After")
        try:
            wait = float(retry_after) if retry_after else base_delay * 2 ** (attempt - 1)
        except ValueError:
            wait = base_delay * 2 ** (attempt - 1)
        wait = min(wait, max_wait) + random.uniform(0, 2)

        if attempt == 1:
            logging.warning("HTTP %d headers: %s", response.status_code, dict(response.headers))
        logging.warning("Attempt %d/%d got HTTP %d; sleeping %.1fs",
                        attempt, max_attempts, response.status_code, wait)
        time.sleep(wait)

class ParseError(ValueError):
    """Response was received but contained no usable product data."""


def parse_data(response) -> pd.DataFrame:
    """Parse a Seoudi or Carrefour response into a flat DataFrame.

    Raises:
        httpx.HTTPStatusError / requests.HTTPError: non-2xx status.
        ParseError: invalid JSON, GraphQL errors, null/empty payload,
            or unrecognised schema.
    """
    response.raise_for_status()  # works for both requests and httpx

    try:
        payload = response.json()
    except ValueError as exc:
        raise ParseError("Body is not valid JSON") from exc

    if payload.get("errors"):
        raise ParseError(f"API returned errors: {payload['errors']}")

    data = payload.get("data") or {}

    if "product" in data:                       # Seoudi schema
        source, records = "Seoudi", data["product"]
    elif data.get("placements"):                # Carrefour schema
        source = "Carrefour"
        records = data["placements"][0].get("recommendedProducts")
    else:
        raise ParseError(f"Unrecognised schema; keys: {list(data)}")

    if not records:
        raise ParseError(f"{source}: product is null/empty (delisted or bad slug?)")

    logging.info("Parsed %s payload (%d top-level entries)", source, len(records))
    return pd.json_normalize(records)


def save_data_to_csv(data: pd.DataFrame, source: str):
    today = date.today().strftime("%m%d%Y")
    filename = f"{source}_{today}.csv"
    os.makedirs(source, exist_ok=True)
    filepath = os.path.join(source, filename)
    data.to_csv(filepath, index=False)


def fetsh_data(tags_data: dict, tag_name_input: str, func: Callable) -> None:
    logging.info("Fetching data for tag: %s", tag_name_input)

    compiled_tags = [
        (tag_values, tag_group_name, category)
        for category, tag_groups in tags_data.items()
        for tag_group_name, tag_values in tag_groups.items()
        if tag_group_name == tag_name_input
    ]

    compiled_data: list[pd.DataFrame] = []
    failures: list[dict] = []

    for tags, tag_group, category in compiled_tags:
        for tag in tags:
            try:
                api_response = func(tag)
                df = parse_data(api_response)
            except Exception as exc:  # per-item boundary; KeyboardInterrupt still propagates
                logging.error(
                    "Failed item %r (category=%s, source=%s): %s: %s",
                    tag, category, tag_group, type(exc).__name__, exc,
                )
                failures.append({
                    "item": tag,
                    "category": category,
                    "source": tag_group,
                    "error_type": type(exc).__name__,
                    "error": str(exc),
                })
                continue

            df["Category"] = category
            df["Source"] = tag_group
            compiled_data.append(df)

    logging.info(
        "%s: %d succeeded, %d failed", tag_name_input, len(compiled_data), len(failures)
    )

    if failures:
        failed_path = os.path.join(
            log_dir, f"failed_{tag_name_input}_{date.today():%m%d%Y}.csv"
        )
        pd.DataFrame(failures).to_csv(failed_path, index=False)
        logging.warning("Failed items written to %s", failed_path)

    if not compiled_data:
        logging.error("No data collected for %s; nothing saved", tag_name_input)
        return

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
