import os
import pandas as pd
import numpy as np
import joblib
from sqlalchemy import create_engine
from snowflake.connector.pandas_tools import write_pandas
from dotenv import load_dotenv

# Load all environment variables from the .env file
load_dotenv()

# Extract configuration values from the environment

SNOWFLAKE_USER = os.getenv("SNOWFLAKE_USER")
SNOWFLAKE_PASSWORD = os.getenv("SNOWFLAKE_PASSWORD")
SNOWFLAKE_ACCOUNT = os.getenv("SNOWFLAKE_ACCOUNT")
SNOWFLAKE_DATABASE = os.getenv("SNOWFLAKE_DATABASE")
SNOWFLAKE_SCHEMA = os.getenv("SNOWFLAKE_SCHEMA")
SNOWFLAKE_WAREHOUSE = os.getenv("SNOWFLAKE_WAREHOUSE")
SNOWFLAKE_ROLE = os.getenv("SNOWFLAKE_ROLE")

# Build the Snowflake SQLAlchemy connection string
connection_string = (
    f"snowflake://{SNOWFLAKE_USER}:{SNOWFLAKE_PASSWORD}@{SNOWFLAKE_ACCOUNT}/"
    f"{SNOWFLAKE_DATABASE}/{SNOWFLAKE_SCHEMA}?warehouse={SNOWFLAKE_WAREHOUSE}&role={SNOWFLAKE_ROLE}"
)
engine = create_engine(connection_string)

def run_inference():
    # Load the model from the local file in the same directory
    model_path = os.path.join(os.getcwd(), "anomaly_model.pkl")
    if not os.path.exists(model_path):
        raise Exception(f"Model file not found at {model_path}")
    print(f"Loading model from {model_path} ...")
    model = joblib.load(model_path)
    print("Model loaded successfully.")

    # Query only the latest data from raw_trades based on the maximum localTS
    query = """
        SELECT * 
        FROM trades_db.trades_schema.raw_trades 
        WHERE localTS = (SELECT MAX(localTS) FROM trades_db.trades_schema.raw_trades)
    """
    print("Querying latest raw_trades data ...")
    df_raw = pd.read_sql(query, engine)
    if df_raw.empty:
        print("No raw trade data found for the latest timestamp. Exiting.")
        return

    # Ensure required columns exist
    if 'price' not in df_raw.columns or 'size' not in df_raw.columns:
        raise Exception("Required columns 'price' and 'size' are missing from raw_trades.")

    # Prepare features for inference using 'price' and 'size'
    features = df_raw[['price', 'size']].copy()
    features.fillna(features.mean(), inplace=True)

    print("Running inference on latest data ...")
    # IsolationForest returns -1 for anomalies, 1 for normal observations
    predictions = model.predict(features)
    df_raw['anomalies'] = np.where(predictions == -1, 'anomaly', 'normal')
    print(f"Inference complete. Processed {len(df_raw)} rows.")

    # Insert the processed (tagged) data into the tagged_trades table
    print("Inserting tagged data into tagged_trades ...")
    conn = engine.raw_connection()
    success, nchunks, nrows, _ = write_pandas(
        conn,
        df_raw,
        table_name="tagged_trades",
        database=SNOWFLAKE_DATABASE,
        schema=SNOWFLAKE_SCHEMA,
        quote_identifiers=False
    )
    if success:
        print(f"Successfully inserted {nrows} rows into tagged_trades (in {nchunks} chunk(s)).")
    else:
        print("Failed to insert tagged data.")
    conn.close()

if __name__ == '__main__':
    run_inference()
