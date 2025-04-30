from prefect import task, flow, get_run_logger
import pandas as pd
from sklearn.model_selection import train_test_split
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import accuracy_score
from sklearn.preprocessing import StandardScaler
import joblib

logger = get_run_logger()


@task
def fetch_data(dataset_path: str) -> pd.DataFrame:
    logger.info(f"Reading data from {dataset_path}")
    df = pd.read_csv(dataset_path)
    logger.info(f"Data shape: {df.shape}")
    return df


@task
def validate_data(df: pd.DataFrame) -> pd.DataFrame:
    logger.info("Validating data")
    missing_values = df.isnull().sum()
    logger.info(f"Missing values:\n{missing_values}")
    # Fill missing numeric values with the median
    df.fillna(df.median(numeric_only=True), inplace=True)
    return df


@task
def transform_data(df: pd.DataFrame) -> pd.DataFrame:
    logger.info("Transforming data")
    # Assume the last column is the target variable.
    features = df.iloc[:, :-1]
    target = df.iloc[:, -1]
    scaler = StandardScaler()
    scaled_features = scaler.fit_transform(features)
    # Reassemble the DataFrame with scaled features
    df_transformed = pd.DataFrame(scaled_features, columns=features.columns)
    df_transformed["target"] = target.values
    return df_transformed


@task(retries=3, retry_delay_seconds=10)
def train_model(df: pd.DataFrame, test_size: float = 0.2, random_state: int = 42):
    logger.info("Training model")
    X = df.drop("target", axis=1)
    y = df["target"]
    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=test_size, random_state=random_state
    )
    model = RandomForestClassifier(random_state=random_state)
    model.fit(X_train, y_train)
    logger.info("Model training complete")
    return model, X_test, y_test


@task
def evaluate_model(model, X_test, y_test) -> float:
    logger.info("Evaluating model")
    predictions = model.predict(X_test)
    acc = accuracy_score(y_test, predictions)
    logger.info(f"Model accuracy: {acc}")
    return acc


@task
def save_model(
    model, accuracy: float, threshold: float, model_path: str = "model.joblib"
):
    if accuracy >= threshold:
        logger.info(
            f"Accuracy {accuracy} meets threshold {threshold}. Saving model to {model_path}"
        )
        joblib.dump(model, model_path)
    else:
        logger.info(
            f"Accuracy {accuracy} below threshold {threshold}. Model not saved."
        )


@flow
def ml_pipeline(
    dataset_path: str = "labs/lab6/iris.csv",
    accuracy_threshold: float = 0.9,
    test_size: float = 0.2,
    model_path: str = "model.joblib",
):

    df = fetch_data(dataset_path)
    df_validated = validate_data(df)
    df_transformed = transform_data(df_validated)
    model, X_test, y_test = train_model(df_transformed, test_size)
    accuracy = evaluate_model(model, X_test, y_test)
    save_model(model, accuracy, accuracy_threshold, model_path)

    logger.info("ML Pipeline completed")


if __name__ == "__main__":
    ml_pipeline()
