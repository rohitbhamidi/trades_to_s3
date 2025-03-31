import os
import sys
import pandas as pd
import joblib

def find_latest_csv(output_folder: str) -> str:
    """
    Returns the full path to the most recently modified CSV file in the output folder.
    """
    files = [f for f in os.listdir(output_folder) if f.lower().endswith('.csv')]
    if not files:
        raise ValueError(f"No CSV files found in the folder: {output_folder}")
    full_paths = [os.path.join(output_folder, f) for f in files]
    latest_file = max(full_paths, key=os.path.getmtime)
    return latest_file

def test_model(input_csv: str, model_path: str, output_csv: str):
    # Load the pre-trained anomaly detection model
    model = joblib.load(model_path)
    
    # Load the CSV file as a batch input
    df = pd.read_csv(input_csv)
    
    # Ensure that the required columns are present
    if 'price' not in df.columns or 'size' not in df.columns:
        print("Error: Required columns ('price', 'size') not found in the input data.")
        sys.exit(1)
    
    # Select the features used by the model (price and size)
    features = df[['price', 'size']].copy()
    features.fillna(features.mean(), inplace=True)
    
    # Predict anomalies using the model
    # IsolationForest returns -1 for anomalies and 1 for normal observations.
    predictions = model.predict(features)
    
    # Add the predictions as a new column to the DataFrame
    df['anomaly'] = predictions
    
    # Print summary information
    total = len(df)
    anomalies = (predictions == -1).sum()
    print(f"Total records processed: {total}")
    print(f"Anomalies detected: {anomalies}")
    
    # Save the results to a new CSV file
    df.to_csv(output_csv, index=False)
    print(f"Predictions saved to {output_csv}")

if __name__ == '__main__':
    # If an input CSV is provided as the first argument, use it.
    # Otherwise, automatically pick the newest CSV file from the ./output folder.
    if len(sys.argv) > 1:
        input_csv = sys.argv[1]
    else:
        input_csv = find_latest_csv("output")
    
    # Use default model and output filenames unless provided.
    model_path = sys.argv[2] if len(sys.argv) > 2 else "anomaly_model.pkl"
    output_csv = sys.argv[3] if len(sys.argv) > 3 else "predictions.csv"
    
    print(f"Using input file: {input_csv}")
    test_model(input_csv, model_path, output_csv)
