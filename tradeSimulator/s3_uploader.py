import boto3
import logging

logger = logging.getLogger(__name__)

class S3Uploader:
    def __init__(self, bucket, region, aws_access_key, aws_secret_key):
        self.bucket = bucket
        self.s3_client = boto3.client(
            's3',
            region_name=region,
            aws_access_key_id=aws_access_key,
            aws_secret_access_key=aws_secret_key
        )

    def upload_file(self, file_path, key):
        try:
            self.s3_client.upload_file(file_path, self.bucket, key)
            logger.info(f"File '{file_path}' uploaded to S3 bucket '{self.bucket}' with key '{key}'.")
        except Exception as e:
            logger.error(f"Error uploading file '{file_path}' to S3: {e}")
            raise
