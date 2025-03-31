import os
import pandas as pd
import random
import time
import logging
from tradeSimulator.config import Config

logger = logging.getLogger(__name__)

class S3Producer:
    def __init__(self):
        from tradeSimulator.s3_uploader import S3Uploader
        self.s3_uploader = S3Uploader(
            bucket=Config.get_s3_bucket(),
            region=Config.get_aws_region(),
            aws_access_key=Config.get_aws_access_key(),
            aws_secret_key=Config.get_aws_secret_key()
        )
        self.output_dir = Config.get_local_csv_output_path()
        os.makedirs(self.output_dir, exist_ok=True)

    def produce_batch(self, trades):
        # Convert trades to a DataFrame
        df = pd.DataFrame(trades)
        
        # Inject anomalies: randomly select between 1 and 10 trades to modify
        anomaly_count = random.randint(1, 10)
        if len(df) > 0:
            anomaly_indices = random.sample(list(df.index), k=min(anomaly_count, len(df)))
            for idx in anomaly_indices:
                factor = random.uniform(2.0, 5.0)
                old_price = df.at[idx, 'price']
                df.at[idx, 'price'] = old_price * factor
                logger.debug(f"Injected anomaly at index {idx}: price changed from {old_price} to {df.at[idx, 'price']}")
        
        # Create a CSV filename with a timestamp
        timestamp_str = time.strftime("%Y%m%d_%H%M%S")
        filename = f"trades_{timestamp_str}.csv"
        file_path = os.path.join(self.output_dir, filename)
        
        # Write DataFrame to CSV file
        df.to_csv(file_path, index=False)
        logger.info(f"CSV file generated: {file_path}")
        
        # Upload the CSV file to S3
        self.s3_uploader.upload_file(file_path, filename)
        logger.info(f"CSV file uploaded to S3 as '{filename}'")

    def close(self):
        pass

class LocalProducer:
    def __init__(self):
        self.output_dir = Config.get_local_csv_output_path()
        os.makedirs(self.output_dir, exist_ok=True)

    def produce_batch(self, trades):
        # Convert trades to a DataFrame
        df = pd.DataFrame(trades)
        
        # Inject anomalies: randomly select between 1 and 10 trades to modify
        anomaly_count = random.randint(1, 10)
        if len(df) > 0:
            anomaly_indices = random.sample(list(df.index), k=min(anomaly_count, len(df)))
            for idx in anomaly_indices:
                factor = random.uniform(2.0, 5.0)
                old_price = df.at[idx, 'price']
                df.at[idx, 'price'] = old_price * factor
                logger.debug(f"Injected anomaly at index {idx}: price changed from {old_price} to {df.at[idx, 'price']}")
        
        # Create a CSV filename with a timestamp
        timestamp_str = time.strftime("%Y%m%d_%H%M%S")
        filename = f"trades_{timestamp_str}.csv"
        file_path = os.path.join(self.output_dir, filename)
        
        # Write DataFrame to CSV file
        df.to_csv(file_path, index=False)
        logger.info(f"CSV file generated locally: {file_path}")

    def close(self):
        pass

def get_producer(mode):
    if mode == "s3":
        return S3Producer()
    elif mode == "local":
        return LocalProducer()
    else:
        raise ValueError("Unsupported mode. Supported modes are 's3' and 'local'.")
