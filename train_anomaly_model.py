import os
import pandas as pd
from sklearn.ensemble import IsolationForest
import joblib
import sys

def load_data_from_output(output_folder: str) -> pd.DataFrame:
    """
    Loads and concatenates all CSV files from the specified output folder.
    """
    files = os.listdir(output_folder)
    data_frames = []
    for filename in files:
        if filename.lower().endswith('.csv'):
            filepath = os.path.join(output_folder, filename)
            df = pd.read_csv(filepath)
            data_frames.append(df)
    if not data_frames:
        raise ValueError(f"No CSV files found in the folder: {output_folder}")
    combined_df = pd.concat(data_frames, ignore_index=True)
    return combined_df

def train_anomaly_model(output_folder: str, model_output: str):
    # Load combined CSV data (files with anomalies)
    df = load_data_from_output(output_folder)
    
    # Ensure required columns are present
    if 'price' not in df.columns or 'size' not in df.columns:
        print("Required columns ('price', 'size') not found in data.")
        sys.exit(1)
    
    # Use 'price' and 'size' as features for anomaly detection
    features = df[['price', 'size']].copy()
    features.fillna(features.mean(), inplace=True)
    
    # Train a simple IsolationForest model
    model = IsolationForest(contamination=0.01, random_state=42)
    model.fit(features)
    
    # Save the model to disk
    joblib.dump(model, model_output)
    print(f"Model trained and saved to {model_output}")

if __name__ == '__main__':
    # Default output folder is 'output' and default model filename is 'anomaly_model.pkl'
    output_folder = sys.argv[1] if len(sys.argv) > 1 else "output"
    model_output = sys.argv[2] if len(sys.argv) > 2 else "anomaly_model.pkl"
    train_anomaly_model(output_folder, model_output)