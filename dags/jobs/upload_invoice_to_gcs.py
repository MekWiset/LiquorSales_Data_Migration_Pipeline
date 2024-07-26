import os
from google.cloud import storage
import logging

from utils.checkpointmanager import CheckpointManager
from utils.configparser import ConfigParser


logging.basicConfig(format='%(levelname)s (%(asctime)s): %(message)s (Line: %(lineno)d) [%(filename)s]', datefmt='%d/%m/%Y %I:%M:%S %p', level=logging.INFO)

config_path = 'config.yaml'
conf = ConfigParser(config_path)

credentials_path = 'credentials/tt-dev-001-419206-4551615b3e2a.json'
bucket_name = 'liquor_sales_bucket'
checkpoint_path = 'checkpoints/gcs_upload_checkpoint.txt'
checkpoint_type = 'gcs'
cp = CheckpointManager(credentials_path, bucket_name, checkpoint_path, checkpoint_type)


def upload_invoice_to_gcs() -> None:

    try:
        credentials_path = conf.gcp_credentials_path('admin')
        storage_client = storage.Client.from_service_account_json(credentials_path)

    except KeyError as e:
        logging.error(f'Credential path for role {e} cannot be found')
    
    else:

        bucket_name = 'liquor_sales_bucket'
        bucket_location = 'asia-east2'

        bucket = storage_client.bucket(bucket_name)
        if not bucket.exists():
            bucket.create(location=bucket_location)


        # Backfill
        backfill_path = 'checkpoints/backfills/gcs_upload_backfill.txt'
        backfill_list = cp.read_backfill(backfill_path=backfill_path)

        if backfill_list is not None:
            for year, month in backfill_list:
                local_backfill_directory = f'outputs/invoice_partitions/Year={year}/Month={month}'
                
                if not os.path.exists(local_backfill_directory):
                    logging.warning(f'Local directory {local_backfill_directory} does not exist. Skipping {year}, {month}.')
                    continue
                
                try:
                    for root, dirs, files in os.walk(local_backfill_directory):
                        for file_name in files:
                            local_file_path = os.path.join(root, file_name)
                            destination_blob_name = f'invoice_partitions/Year={year}/Month={month}/{file_name}'
                            blob = bucket.blob(destination_blob_name)
                            blob.upload_from_filename(local_file_path)
                            logging.info(f'{file_name} has been successfully uploaded into Google Cloud Storage in {bucket_name}')
                    
                    cp.update_checkpoint(year, month, 'UPLOADED')
                    cp.remove_backfill(backfill_path, year, month)

                except Exception as e:
                    logging.info(f'Upload failed for {year}-{month}: {str(e)}')
                    cp.update_checkpoint(year, month, 'NOT UPLOADED')
                    cp.write_backfill(backfill_path, year, month)
                    continue


        # Current Upload
        current_year, current_month = cp.get_current_partition()
        local_directory = f'outputs/invoice_partitions/Year={current_year}/Month={current_month}'

        try:
            if not os.path.exists(local_directory):
                raise FileNotFoundError(f'Local directory {local_directory} does not exist.')

            for root, _, files in os.walk(local_directory):
                for file_name in files:
                    local_file_path = os.path.join(root, file_name)
                    destination_blob_name = f'invoice_partitions/Year={current_year}/Month={current_month}/{file_name}'
                    blob = bucket.blob(destination_blob_name)
                    blob.upload_from_filename(local_file_path)
                    logging.info(f'{file_name} has been successfully uploaded into Google Cloud Storage in {bucket_name}')

            cp.update_checkpoint(current_year, current_month, 'UPLOADED')

        except Exception as e:
            logging.error(f'Upload failed for current directory {current_year}-{current_month}: {str(e)}')
            cp.update_checkpoint(current_year, current_month, 'NOT UPLOADED')
            cp.write_backfill(backfill_path, current_year, current_month)


if __name__ == '__main__':

    upload_invoice_to_gcs()

