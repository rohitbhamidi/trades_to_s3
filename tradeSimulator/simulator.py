import logging
import pandas as pd
import time
import sys
import random
from datetime import datetime
from tradeSimulator.config import Config
from tradeSimulator.logger_config import setup_logging
from tradeSimulator.producer import get_producer

logger = logging.getLogger(__name__)

def load_data() -> pd.DataFrame:
    df = pd.read_csv(Config.get_local_csv_path())
    required_cols = [
        "localTS", "localDate", "ticker", "conditions", "correction", "exchange",
        "id", "participant_timestamp", "price", "sequence_number", "sip_timestamp",
        "size", "tape", "trf_id", "trf_timestamp"
    ]
    for col in required_cols:
        if col not in df.columns:
            df[col] = 0
            logger.warning(f"Column '{col}' not found in CSV. Created dummy column with default values.")
    df['conditions'] = df['conditions'].fillna('')
    df['conditions'] = df['conditions'].astype(str)
    return df

def simulate_trades(mode: str):
    # Load base trade data from CSV
    df = load_data()
    producer = get_producer(mode)
    
    try:
        while True:
            # Generate a random number of rows between 1000 and 5000
            num_rows = random.randint(1000, 5000)
            batch = df.sample(n=num_rows, replace=True).copy()
            
            # Update timestamps and other time-dependent fields
            current_dt = datetime.now()
            current_ts_str = current_dt.strftime("%Y-%m-%d %H:%M:%S")
            current_date_str = current_dt.strftime("%Y-%m-%d")
            current_ts_ns = int(current_dt.timestamp() * 1_000_000_000)
            
            batch['localTS'] = current_ts_str
            batch['localDate'] = current_date_str
            batch['participant_timestamp'] = current_ts_ns
            batch['sip_timestamp'] = current_ts_ns
            batch['trf_timestamp'] = current_ts_ns
            
            # Convert the DataFrame to a list of dictionaries
            trades_list = batch.to_dict(orient='records')
            
            # Produce the batch (generate CSV, inject anomalies, and either upload to S3 or write locally)
            producer.produce_batch(trades_list)
            
            # Countdown for one minute before generating the next CSV file
            for remaining in range(60, 0, -1):
                sys.stdout.write("\r")
                sys.stdout.write(f"Next CSV in: {remaining} seconds")
                sys.stdout.flush()
                time.sleep(1)
            print()  # Move to a new line after the countdown
    except KeyboardInterrupt:
        logger.info("Stopping simulation due to keyboard interrupt.")
    finally:
        producer.close()
        logger.info("Simulation ended.")

def main():
    setup_logging()
    logger.info("Starting trade simulation...")
    simulate_trades(mode=Config.get_mode())
    logger.info("Trade simulation completed.")

if __name__ == '__main__':
    main()
