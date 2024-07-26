from google.cloud import bigquery
from google.cloud import storage
from utils.configparser import ConfigParser
from typing import List
import logging


logging.basicConfig(format='%(levelname)s (%(asctime)s): %(message)s (Line: %(lineno)d) [%(filename)s]', datefmt='%d/%m/%Y %I:%M:%S %p', level=logging.INFO)

config_path = 'config.yaml'
conf = ConfigParser(config_path)


def upload_gcs_to_bq(bucket_name: str, tables: List[str]) -> None:

    try:
        credentials_path = conf.gcp_credentials_path('admin')
        client = bigquery.Client.from_service_account_json(credentials_path)
        storage_client = storage.Client.from_service_account_json(credentials_path)

    except KeyError as e:
        logging.error(f'Unable to access Google Cloud Platform using {e} role')

    else:
        
        parquet_dir_table_pairs = [conf.dir_table_pair(table) for table in tables]

        for parquet_dir_path, bq_table_id in parquet_dir_table_pairs:
            
            bucket_name = 'liquor_sales_bucket'

            bucket = storage_client.bucket(bucket_name)
            blobs = list(bucket.list_blobs(prefix=parquet_dir_path))

            job_config = bigquery.LoadJobConfig(
                autodetect='TRUE',
                source_format=bigquery.SourceFormat.PARQUET
            )

            try:
                client.get_table(bq_table_id)
                logging.info(f'Table {bq_table_id} found.')
                logging.info(f'Inserting data from {parquet_dir_path}...')
                loaded_any_file = False
                for blob in blobs:
                    if blob.name.endswith('.parquet'):
                        job = client.load_table_from_uri(blob.public_url, bq_table_id, job_config=job_config)
                        job.result()
                        loaded_any_file = True
                        logging.info(f'Loaded {job.output_rows} rows from {blob.name} into {bq_table_id}.')

                if not loaded_any_file:
                    logging.info(f'No Parquet files found in {parquet_dir_path}.')

            except Exception:
                logging.info(f'Table {bq_table_id} not found.')
                logging.info(f'Attempting to create table and insert data from {parquet_dir_path}...')
                loaded_any_file = False
                for blob in blobs:
                    if blob.name.endswith('.parquet'):
                        blob_uri = f'gs://{bucket_name}/{blob.name}'
                        job = client.load_table_from_uri(blob_uri, bq_table_id, job_config=job_config)
                        job.result()
                        loaded_any_file = True
                        logging.info(f'Loaded {job.output_rows} rows from {blob.name} into {bq_table_id}.')

                if not loaded_any_file:
                    logging.info(f'No Parquet files found in {parquet_dir_path}.')
                    

if __name__ == '__main__':

    bucket_name = 'liquor_sales_bucket'
    tables = [
        'item_number_bridge', 'category', 'vendor', 'item_price_history', 'item',
        'store_number_bridge', 'store_address_bridge', 'store_county',
        'store_address_history', 'store_address'
    ]

    upload_gcs_to_bq(bucket_name=bucket_name, tables=tables)

    