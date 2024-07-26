import os
from google.cloud import storage
from utils.configparser import ConfigParser
import logging


logging.basicConfig(format='%(levelname)s (%(asctime)s): %(message)s (Line: %(lineno)d) [%(filename)s]', datefmt='%d/%m/%Y %I:%M:%S %p', level=logging.INFO)

config_path = 'config.yaml'
conf = ConfigParser(config_path)


def upload_to_gcs(local_dir: str, bucket: str) -> None:

    try:
        credentials_path = conf.gcp_credentials_path('admin')
        storage_client = storage.Client.from_service_account_json(credentials_path)

    except KeyError as e:
        logging.error(f'Credential path for role {e} cannot be found')
    
    else:
        bucket_name, bucket_location = conf.bucket_info(bucket)

        bucket = storage_client.bucket(bucket_name)
        if not bucket.exists():
            bucket.create(location=bucket_location)
        
        local_directory = local_dir
    
        for root, dirs, files in os.walk(local_directory):
            for file_name in files:
                local_file_path = os.path.join(root, file_name)
                destination_blob_name = os.path.relpath(local_file_path, local_directory)
                blob = bucket.blob(destination_blob_name)
                blob.upload_from_filename(local_file_path)

                logging.info(f'{file_name} has been successfully uploaded into Google Cloud Storage in {bucket_name}')

    return None


if __name__ == '__main__':

    local_dir = 'outputs'
    bucket = 'liquor_sales'
    
    upload_to_gcs(local_dir=local_dir, bucket=bucket)

