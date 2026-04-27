import pandas as pd
import os
import logging
from datetime import date
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError

# ── Logging ────────────────────────────────────────────────────────────────────
log_dir = "log_files"
os.makedirs(log_dir, exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler(
            os.path.join(log_dir, f"cleaner_{date.today().strftime('%m%d%Y')}.log"),
            mode="a",
        ),
        logging.StreamHandler(),
    ],
)

# ── DB config from environment variables (set in docker-compose) ───────────────
DB_HOST = os.environ.get("DB_HOST")
DB_PORT = os.environ.get("DB_PORT")
DB_NAME = os.environ.get("DB_NAME")
DB_USER = os.environ.get("DB_USER")
DB_PASS = os.environ.get("DB_PASS")

TABLE_NAME = "prices"


def get_engine():
    url = f"postgresql+psycopg2://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
    return create_engine(url)


# ── Reading ────────────────────────────────────────────────────────────────────
def read_datafiles(directory: str) -> list[pd.DataFrame]:
    data_files = []
    for file in os.listdir(directory):
        if not file.endswith(".csv"):
            continue
        file_path = os.path.join(directory, file)
        logging.info(f"Reading: {file_path}")
        df = pd.read_csv(file_path)
        data_files.append(df)
    if not data_files:
        logging.warning(f"No CSV files found in {directory}")
    return data_files


# ── Cleaning ───────────────────────────────────────────────────────────────────
def clean_data(
    df: pd.DataFrame,
    product_column_name: str,
    weight_column_name: str,
    price_column_name: str,
) -> pd.DataFrame:
    for col in [product_column_name, weight_column_name, price_column_name]:
        if col not in df.columns:
            raise ValueError(f"Column '{col}' does not exist in DataFrame")

    df = df.copy()
    df["Product_Name"] = df.apply(
        lambda row: (
            row[product_column_name]
            if pd.isna(row[weight_column_name])
            else f"{row[product_column_name]}_{row[weight_column_name]}"
        ),
        axis=1,
    )

    new_df = df[["Date", "Product_Name", price_column_name, "Source", "Category"]].copy()
    new_df.columns = ["Date", "Product_Name", "Price", "Source", "Category"]
    new_df["Date"] = pd.to_datetime(new_df["Date"], format="%m/%d/%Y")
    new_df["Price"] = pd.to_numeric(new_df["Price"], errors="coerce")
    new_df.dropna(subset=["Price", "Product_Name"], inplace=True)

    return new_df


# ── Deduplication ──────────────────────────────────────────────────────────────
def filter_existing_records(df: pd.DataFrame, engine) -> pd.DataFrame:
    """
    Fetch existing (Date, Product_Name, Source) combos from DB and
    remove any rows in df that already exist — prevents duplicate inserts.
    """
    try:
        with engine.connect() as conn:
            existing = pd.read_sql(
                text(f"SELECT date, product_name, source FROM {TABLE_NAME}"),
                conn,
            )
    except SQLAlchemyError:
        # Table doesn't exist yet on first run — nothing to deduplicate against
        logging.info("No existing table found — skipping deduplication.")
        return df

    if existing.empty:
        return df

    existing["date"] = pd.to_datetime(existing["date"])
    existing_keys = set(
        zip(existing["date"], existing["product_name"], existing["source"])
    )

    before = len(df)
    df = df[
        ~df.apply(
            lambda row: (row["Date"], row["Product_Name"], row["Source"]) in existing_keys,
            axis=1,
        )
    ]
    after = len(df)
    logging.info(f"Deduplication: {before - after} duplicate rows removed, {after} new rows to insert.")
    return df


# ── DB Table Init ──────────────────────────────────────────────────────────────
def ensure_table_exists(engine):
    create_sql = text(f"""
        CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
            id          SERIAL PRIMARY KEY,
            date        DATE        NOT NULL,
            product_name TEXT       NOT NULL,
            price       NUMERIC     NOT NULL,
            source      TEXT        NOT NULL,
            category    TEXT,
            inserted_at TIMESTAMP   DEFAULT NOW(),
            UNIQUE (date, product_name, source)
        );
    """)
    with engine.begin() as conn:
        conn.execute(create_sql)
    logging.info(f"Table '{TABLE_NAME}' is ready.")


# ── Upload ─────────────────────────────────────────────────────────────────────
def upload_to_postgres(df: pd.DataFrame, engine):
    if df.empty:
        logging.info("No new data to upload.")
        return

    df_upload = df.rename(columns={
        "Date": "date",
        "Product_Name": "product_name",
        "Price": "price",
        "Source": "source",
        "Category": "category",
    })

    try:
        df_upload.to_sql(
            TABLE_NAME,
            engine,
            if_exists="append",   # never replace — always append
            index=False,
            method="multi",       # batched insert, faster on Pi
        )
        logging.info(f"Successfully uploaded {len(df_upload)} rows to '{TABLE_NAME}'.")
    except SQLAlchemyError as e:
        logging.error(f"Upload failed: {e}")
        raise


# ── Load & Process ─────────────────────────────────────────────────────────────
def load_data() -> pd.DataFrame:
    directories = {
        "Seoudi_tags": [
            "name",
            "weight_base_unit",
            "price_range.maximum_price.regular_price.value",
        ],
        "Carrefour_tags": [
            "name",
            "unit.unitOfMeasure",
            "price.price",
        ],
    }

    all_data = []
    for directory, items in directories.items():
        dfs = read_datafiles(directory)
        if not dfs:
            continue
        df = pd.concat(dfs, ignore_index=True)
        cleaned = clean_data(df, *items)
        all_data.append(cleaned)

    if not all_data:
        logging.warning("No data loaded from any directory.")
        return pd.DataFrame()

    return pd.concat(all_data, ignore_index=True)


# ── Entry Point ────────────────────────────────────────────────────────────────
def run():
    logging.info("=== Cleaner started ===")

    engine = get_engine()
    ensure_table_exists(engine)

    df = load_data()
    if df.empty:
        logging.info("Nothing to process.")
        return

    logging.info(f"Total rows loaded: {len(df)}")

    df = filter_existing_records(df, engine)
    upload_to_postgres(df, engine)

    logging.info("=== Cleaner finished ===")


if __name__ == "__main__":
    run()
