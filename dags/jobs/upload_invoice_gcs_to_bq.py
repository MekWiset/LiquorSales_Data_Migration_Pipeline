from google.cloud import bigquery
from google.cloud import storage
import logging

from utils.configparser import ConfigParser
from utils.checkpointmanager import CheckpointManager


logging.basicConfig(format='%(levelname)s (%(asctime)s): %(message)s (Line: %(lineno)d) [%(filename)s]', datefmt='%d/%m/%Y %I:%M:%S %p', level=logging.INFO)

config_path = 'config.yaml'
conf = ConfigParser(config_path)

credentials_path = 'credentials/tt-dev-001-419206-4551615b3e2a.json'
bucket_name = 'liquor_sales_bucket'
checkpoint_path = 'checkpoints/bigquery_upload_checkpoint.txt'
checkpoint_type = 'bigquery'
cp = CheckpointManager(credentials_path, bucket_name, checkpoint_path, checkpoint_type)


def upload_invoice_gcs_to_bq() -> None:

    try:
        credentials_path = conf.gcp_credentials_path('admin')
        client = bigquery.Client.from_service_account_json(credentials_path)
        storage_client = storage.Client.from_service_account_json(credentials_path)

    except KeyError as e:
        logging.error(f'Unable to access Google Cloud Platform using {e} role')

    else:

        # Backfill
        backfill_path = 'checkpoints/backfills/bigquery_upload_backfill.txt'
        backfill_list = cp.read_backfill(backfill_path=backfill_path)

        if backfill_list is not None:
            for year, month in backfill_list:
                gcs_backfill_directory = f'invoice_partitions/Year={year}/Month={month}/'
                blobs = list(bucket.list_blobs(prefix=gcs_backfill_directory))

                if not blobs:
                    logging.warning(f'No blobs found in GCS directory {gcs_backfill_directory}. Skipping {year}, {month}.')
                    cp.update_checkpoint(year, month, 'NOT UPLOADED')
                    continue

                logging.info(f'Uploading data from {gcs_backfill_directory} to BigQuery...')


        # Current Upload
        current_year, current_month = cp.get_current_partition()

        parquet_dir_path = f'invoice_partitions/Year={current_year}/Month={current_month}/'

        bucket_name = 'liquor_sales_bucket'

        bucket = storage_client.bucket(bucket_name)
        blobs = list(bucket.list_blobs(prefix=parquet_dir_path))

        job_config = bigquery.LoadJobConfig(
            autodetect='TRUE',
            source_format=bigquery.SourceFormat.PARQUET
        )

        bq_table_id = 'tt-dev-001-419206.Liquor_sales_data.Invoice'

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
        
        finally:
            cp.update_checkpoint(current_year, current_month, 'UPLOADED')


if __name__ == '__main__':

    upload_invoice_gcs_to_bq()

