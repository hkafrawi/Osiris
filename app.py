import streamlit as st
import pandas as pd
import matplotlib.pyplot as plt
import os


# -------------------------------
# Read, Clean and Load Data
# -------------------------------
def read_datafiles(directory):
    data_files = []
    for file in os.listdir(directory):
        file_path = os.path.join(directory, file)
        df = pd.read_csv(file_path)
        data_files.append(df)

    return data_files


def clean_data(df, product_column_name, weight_column_name, price_column_name):
    if product_column_name not in df.columns:
        raise ValueError("Product name column does not exist in DataFrame")
    if weight_column_name not in df.columns:
        raise ValueError("Weight unit column does not exist in DataFrame")
    if price_column_name not in df.columns:
        raise ValueError("Price column does not exist in DataFrame")
    df["Product_Name"] = df.apply(
        lambda row: (
            row[product_column_name]
            if pd.isna(row[weight_column_name])
            else f"{row[product_column_name]}_{row[weight_column_name]}"
        ),
        axis=1,
    )
    new_df = df[["Date", "Product_Name", price_column_name, "Source"]]
    new_df.columns = ["Date", "Product_Name", "Price", "Source"]
    new_df["Date"] = pd.to_datetime(new_df["Date"], format="%m/%d/%Y")
    return new_df


@st.cache_data
def load_data():
    all_data = []
    directories = {
        "Seoudi_tags": [
            "name",
            "weight_base_unit",
            "price_range.maximum_price.regular_price.value",
        ],
        "Carrefour_tags": ["name", "unit.unitOfMeasure", "price.price"],
    }
    for directory, items in directories.items():
        all_dfs = read_datafiles(directory)
        df = pd.concat(all_dfs)
        cleaned_df = clean_data(df, *items)
        all_data.append(cleaned_df)
    final_df = pd.concat(all_data)
    return final_df


df = load_data()

# -------------------------------
# Sidebar filters
# -------------------------------
st.sidebar.header("Filters")

product_options = df["Product_Name"].unique().tolist()
source_options = df["Source"].unique().tolist()

product = st.sidebar.selectbox("Select product", product_options)
source = st.sidebar.selectbox("Select source", source_options)

min_date = df["Date"].min()
max_date = df["Date"].max()
date_range = st.sidebar.date_input(
    "Select date range", [min_date, max_date], min_value=min_date, max_value=max_date
)

# -------------------------------
# Filtered Data
# -------------------------------
mask = (
    (df["Product_Name"] == product)
    & (df["Source"] == source)
    & (df["Date"].between(pd.to_datetime(date_range[0]), pd.to_datetime(date_range[1])))
)

filtered_df = df.loc[mask]

# -------------------------------
# Visualization
# -------------------------------
st.title("Price Tracking Dashboard")

if filtered_df.empty:
    st.warning("No data available for the selected filters.")
else:
    fig, ax = plt.subplots(figsize=(8, 4))
    ax.plot(filtered_df["Date"], filtered_df["Price"], marker="o")
    ax.set_xlabel("Date")
    ax.set_ylabel("Price")
    ax.set_title(f"{product} price trend ({source})")
    plt.xticks(rotation=45)

    st.pyplot(fig)
