# extract_s3.py
import boto3
import os

def download_from_s3(bucket_name, object_key, download_path):
    s3 = boto3.client('s3')
    os.makedirs(os.path.dirname(download_path), exist_ok=True)

    # Delete the file if it exists to force overwrite
    if os.path.exists(download_path):
        os.remove(download_path)

    s3.download_file(bucket_name, object_key, download_path)
    print(f"Downloaded {object_key} to {download_path}")