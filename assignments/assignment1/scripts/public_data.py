import requests
import os
import pandas as pd

API_URL = "https://api.coingecko.com/api/v3/coins/markets"
PARAMS = {
    "vs_currency": "usd",
    "order": "market_cap_desc",
    "per_page": 100,
    "page": 1,
    "sparkline": "false",
}

RAW_OUTPUT_FILE = "./datasets/raw/crypto_public_data.csv"
CLEAN_OUTPUT_FILE = "./datasets/clean/crypto_public_data_cleaned.csv"


def fetch_crypto_data():
    try:
        print("🔄 Fetching real-time crypto data...")
        response = requests.get(API_URL, params=PARAMS)
        response.raise_for_status()  # Raise error for bad status codes

        data = response.json()
        if not data:
            print("⚠️ No data received from API.")
            return None

        print(f"✅ Retrieved {len(data)} records.")
        return pd.DataFrame(data)

    except requests.exceptions.RequestException as e:
        print(f"❌ Error: Failed to fetch data. Details: {e}")
        return None


def save_to_csv(data, filename):
    if data is None or data.empty:
        print("⚠️ No data to save.")
        return

    os.makedirs(os.path.dirname(filename), exist_ok=True)
    data.to_csv(filename, index=False)
    print(f"✅ Data successfully saved to {filename}")


def clean_data(input_file, output_file):
    try:
        data = pd.read_csv(input_file)

        if data.empty:
            print("⚠️ No data available to clean.")
            return

        columns_to_keep = [
            "id",
            "symbol",
            "name",
            "current_price",
            "market_cap",
            "total_volume",
        ]
        data = data[columns_to_keep]

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

        summary = data.agg(
            {
                "current_price": ["mean", "max", "min"],
                "market_cap": ["mean", "max", "min"],
                "total_volume": ["mean", "max", "min"],
            }
        )

        print("\n📊 Crypto Market Data Summary 📊")
        print(summary)

    except Exception as e:
        print(f"❌ Error summarizing data: {e}")


### **Run the Pipeline**
if __name__ == "__main__":
    crypto_data = fetch_crypto_data()

    if crypto_data is not None:
        save_to_csv(crypto_data, RAW_OUTPUT_FILE)
        clean_data(RAW_OUTPUT_FILE, CLEAN_OUTPUT_FILE)
        summarize_data(CLEAN_OUTPUT_FILE)
