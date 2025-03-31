import os
from dotenv import load_dotenv

# Load environment variables from a .env file if available
load_dotenv()

class Config:
    @staticmethod
    def get_log_level():
        return os.getenv("LOG_LEVEL", "INFO")

    # CSV input file with base trade data
    @staticmethod
    def get_local_csv_path():
        return os.getenv("LOCAL_CSV_PATH", "./trades_data.csv")

    # Local output directory for generated CSV files
    @staticmethod
    def get_local_csv_output_path():
        return os.getenv("LOCAL_CSV_OUTPUT_PATH", "./output")

    @staticmethod
    def get_log_interval():
        return int(os.getenv("LOG_INTERVAL", "5"))

    # S3 configuration (used only when mode is "s3")
    @staticmethod
    def get_s3_bucket():
        return os.getenv("S3_BUCKET")

    @staticmethod
    def get_aws_access_key():
        return os.getenv("AWS_ACCESS_KEY_ID")

    @staticmethod
    def get_aws_secret_key():
        return os.getenv("AWS_SECRET_ACCESS_KEY")

    @staticmethod
    def get_aws_region():
        return os.getenv("AWS_REGION", "us-east-1")

    # Mode: set to "local" for testing CSV generation locally,
    # or "s3" to generate and upload to S3.
    @staticmethod
    def get_mode():
        return os.getenv("MODE", "local")
