import time
import os
import pandas as pd  # For saving data as CSV
from pytrends.request import TrendReq  # Google Trends API

# Define constants
KEYWORDS = ["Bitcoin", "Ethereum", "Crypto regulation"]
# KEYWORDS = ["Bitcoin"]
TIMEFRAME = "today 12-m"  # Last 12 months
REGION = "US"  # Set to "US" for region-based analysis, or leave blank for worldwide
RAW_OUTPUT_FILE = "./datasets/raw/pytrends.csv"
CLEAN_OUTPUT_FILE = "./datasets/clean/pytrends.csv"


def fetch_google_trends(keywords, timeframe, region):
    """Fetches interest data from Google Trends for given keywords."""
    try:
        print(f"🔄 Fetching Google Trends data for {keywords}...")
        pytrends = TrendReq()

        time.sleep(20)

        pytrends.build_payload(kw_list=keywords, timeframe=timeframe, geo=region)
        data = pytrends.interest_over_time()

        if data.empty:
            print("⚠️ No data found for the given keywords.")
            return None

        data.reset_index(inplace=True)
        data = data.melt(
            id_vars=["date"], var_name="Keyword", value_name="Interest Score"
        )
        data.rename(columns={"date": "Date"}, inplace=True)

        print(f"✅ Fetched {len(data)} records.")
        return data

    except Exception as e:
        print(f"❌ Error: Failed to fetch data. Details: {e}")
        return None


def save_to_csv(data, filename):
    """Saves extracted Google Trends data to a CSV file."""
    if data is None or data.empty:
        print("⚠️ No data to save.")
        return

    os.makedirs(os.path.dirname(filename), exist_ok=True)
    data.to_csv(filename, index=False)

    print(f"✅ Data successfully saved to {filename}")


def clean_data(input_file, output_file):
    """Cleans the Google Trends data: removes negative values and missing scores."""
    try:
        data = pd.read_csv(input_file)

        if data.empty:
            print("⚠️ No data available to clean.")
            return

        data = data[data["Interest Score"] >= 0]

        data.dropna(inplace=True)

        save_to_csv(data, output_file)
        print(f"✅ Cleaned data saved to {output_file}")

    except Exception as e:
        print(f"❌ Error cleaning data: {e}")


def summarize_data(filename):
    try:
        data = pd.read_csv(filename)

        if data.empty:
            print("⚠️ No data available for summary.")
            return

        summary = (
            data.groupby("Keyword")["Interest Score"]
            .agg(["mean", "max", "min"])
            .reset_index()
        )
        print("\n📊 Google Trends Data Summary 📊")
        print(summary)

    except Exception as e:
        print(f"❌ Error summarizing data: {e}")


### **Run the Pipeline**
if __name__ == "__main__":
    trends_data = fetch_google_trends(KEYWORDS, TIMEFRAME, REGION)

    if trends_data is not None:
        save_to_csv(trends_data, RAW_OUTPUT_FILE)
        clean_data(RAW_OUTPUT_FILE, CLEAN_OUTPUT_FILE)
        summarize_data(CLEAN_OUTPUT_FILE)
