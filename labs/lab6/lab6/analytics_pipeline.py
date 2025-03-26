from prefect import task, flow
import pandas as pd
import matplotlib.pyplot as plt
import logging

logger = logging.getLogger(__name__)


@task
def fetch_data(file_path: str) -> pd.DataFrame:
    """
    Step 1: Fetch Data from the provided file path
    """
    logger.info(f"Reading data from {file_path}...")
    df = pd.read_csv(file_path)
    logger.info(f"Data shape: {df.shape}")
    return df


@task
def validate_data(df: pd.DataFrame) -> pd.DataFrame:
    """
    Step 2: Validate Data
    """
    logger.info("Validating data...")
    missing_values = df.isnull().sum()
    logger.info(f"Missing values:\n{missing_values}")

    # For simplicity, drop any rows with missing values
    df_clean = df.dropna()
    return df_clean


@task
def transform_data(df: pd.DataFrame) -> pd.DataFrame:
    """
    Step 3: Transform Data
    """
    logger.info("Transforming data...")
    if "sales" in df.columns:
        df["sales_normalized"] = (df["sales"] - df["sales"].mean()) / df["sales"].std()
        logger.info("Sales column normalized.")
    return df


@task
def generate_analytics_report(df: pd.DataFrame, output_path: str):
    """
    Step 4: Analytics Report
    """
    logger.info("Generating analytics report...")
    summary = df.describe()
    summary.to_csv(output_path)
    logger.info(f"Summary statistics saved to {output_path}")


@task
def create_sales_histogram(df: pd.DataFrame, output_path: str):
    """
    Step 5: Histogram for Sales Distribution
    """
    if "sales" in df.columns:
        logger.info("Creating sales histogram...")
        plt.hist(df["sales"], bins=20)
        plt.title("Sales Distribution")
        plt.xlabel("Sales")
        plt.ylabel("Frequency")
        plt.savefig(output_path)
        plt.close()
        logger.info(f"Sales histogram saved to {output_path}")


@flow
def analytics_pipeline(
    file_path: str, summary_output_path: str, histogram_output_path: str
):
    df = fetch_data(file_path)
    df_clean = validate_data(df)
    df_transformed = transform_data(df_clean)
    generate_analytics_report(df_transformed, summary_output_path)
    create_sales_histogram(df_transformed, histogram_output_path)
    logger.info("Analytics pipeline completed.")


if __name__ == "__main__":
    analytics_pipeline(
        file_path="labs/lab6/analytics_data.csv",
        summary_output_path="labs/lab6/analytics_summary.csv",
        histogram_output_path="labs/lab6/sales_histogram.png",
    )
