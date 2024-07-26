from datetime import datetime
from dateutil.relativedelta import relativedelta
from google.cloud import storage


class CheckpointManager:

    '''
    Manages checkpoints stored in Google Cloud Storage (GCS).

    Args:
        credentials_path (str): The path to the Google Cloud service account JSON credentials.
        bucket_name (str): The name of the GCS bucket.
        checkpoint_path (str): The path to the checkpoint file in the bucket.
        checkpoint_type (str): The type of checkpoint (invoice, gcs, or bigquery).

    Methods:
        set_checkpoint_type(self, checkpoint_type: str) -> None:
            Sets the checkpoint type based on the provided string. Options are: 'invoice', 'gcs', 'bigquery'.
        
        read_latest_checkpoint(self) -> tuple[int, int]:
            Reads the latest checkpoint from the GCS bucket.
        
        get_current_partition(self) -> tuple[int, int]:
            Calculates the current partition based on the latest checkpoint.
        
        update_checkpoint(self, current_year: int, current_month: int, status: str) -> None:
            Updates the checkpoint file with the given year, month, and status.
        
        read_backfill(self, backfill_path: str) -> list[list[int]]:
            Reads the backfill file from the GCS bucket and returns a list of year, month pairs.
        
        remove_backfill(self, backfill_path: str, year: int, month: int) -> None:
            Removes a specific entry from the backfill file after successful upload.
        
        write_backfill(self, backfill_path: str, current_year: int, current_month: int) -> None:
            Writes an entry to the backfill file indicating a failed upload attempt.
    
    Attributes:
        client (storage.Client): The GCS client.
        bucket (storage.Bucket): The GCS bucket.
        checkpoint_file (str): The path to the checkpoint file in the bucket.
        checkpoint_type (str): The type of checkpoint.
    '''
    
    def __init__(self, credentials_path, bucket_name, checkpoint_path, checkpoint_type):
        self.client = storage.Client.from_service_account_json(credentials_path)
        self.bucket = self.client.bucket(bucket_name)
        self.checkpoint_file = checkpoint_path
        self.set_checkpoint_type(checkpoint_type)


    def set_checkpoint_type(self, checkpoint_type: str) -> None:
        '''Sets the checkpoint type based on the provided string. Options are: 'invoice', 'gcs', 'bigquery'.'''
        if checkpoint_type == 'invoice':
            self.checkpoint_type = 'INVOICE_CHECKPOINT'
        elif checkpoint_type == 'gcs':
            self.checkpoint_type = 'GCS_UPLOAD_CHECKPOINT'
        elif checkpoint_type == 'bigquery':
            self.checkpoint_type = 'BIGQUERY_UPLOAD_CHECKPOINT'
        else:
            raise ValueError("Invalid checkpoint type. Supported types are ['invoice', 'gcs', 'bigquery'].")


    def read_latest_checkpoint(self) -> tuple[int, int]:
        '''Reads the latest checkpoint from the GCS bucket.'''
        blob = self.bucket.blob(self.checkpoint_file)
        try:
            if blob.exists():
                content = blob.download_as_text().strip().split('\n')
                if content:
                    last_line = content[-1]
                    if last_line.startswith(f'{self.checkpoint_type} ='):
                        checkpoint_data = last_line.split('=')[1].strip().split(',')
                        latest_year = int(checkpoint_data[0])
                        latest_month = int(checkpoint_data[1].split()[0])  # Extract month before status
                        return latest_year, latest_month
                    else:
                        raise ValueError(f'{self.checkpoint_type} not found in {self.checkpoint_file}.')
                else:  # for empty file -> return default latest value
                    return 2011, 12
            else:  # for file not found -> return default latest value
                return 2011, 12
        except Exception as e:
            raise e


    def get_current_partition(self) -> tuple[int, int]:
        '''Calculates the current partition based on the latest checkpoint.'''
        latest_year, latest_month = self.read_latest_checkpoint()
        latest_date = datetime(latest_year, latest_month, 1)
        current_date = latest_date + relativedelta(months=1)
        return current_date.year, current_date.month


    def update_checkpoint(self, current_year: int, current_month: int, status: str) -> None:
        '''Updates the checkpoint file with the given year, month, and status.'''
        blob = self.bucket.blob(self.checkpoint_file)
        try:
            if blob.exists():
                content = blob.download_as_text().strip().split('\n')
                updated = False
                for i, line in enumerate(content):
                    if line.startswith(f'{self.checkpoint_type} = {current_year},{current_month}'):
                        content[i] = f'{self.checkpoint_type} = {current_year},{current_month} ({status})'
                        updated = True
                        break
                if not updated:
                    content.append(f'{self.checkpoint_type} = {current_year},{current_month} ({status})')
                new_content = '\n'.join(content)
            else:
                new_content = f'{self.checkpoint_type} = {current_year},{current_month} ({status})'
            blob.upload_from_string(new_content)
        except Exception as e:
            raise e


    def read_backfill(self, backfill_path: str) -> list[list[int]]:    # [[2012, 2], [2012, 2], ...]
        '''Reads the backfill file from the GCS bucket and returns a list of year, month pairs.'''
        blob = self.bucket.blob(backfill_path)
        if blob.exists():
            content = blob.download_as_text().strip().split('\n')
            backfill_list = []
            for line in content:
                if line.startswith(f'{self.checkpoint_type.upper()}_BACKFILL ='):
                    checkpoint_data = line.split('=')[1].strip().split(',')
                    year = int(checkpoint_data[0])
                    month = int(checkpoint_data[1].split()[0])
                    backfill_list.append([year, month])
            return backfill_list
        else:
            return []
        

    def remove_backfill(self, backfill_path: str, year: int, month: int) -> None:
        '''Removes a specific entry from the backfill file after successful upload.'''
        blob = self.bucket.blob(backfill_path)
        try:
            if blob.exists():
                content = blob.download_as_text().strip().split('\n')
                target_line = f'{self.checkpoint_type}_BACKFILL = {year},{month} (NOT UPLOADED)'
                new_content = [line for line in content if line.strip() != target_line]
                new_blob_content = '\n'.join(new_content)
                blob.upload_from_string(new_blob_content)
        except Exception as e:
            raise e


    def write_backfill(self, backfill_path: str, current_year: int, current_month: int) -> None:
        '''Writes an entry to the backfill file indicating a failed upload attempt.'''
        blob = self.bucket.blob(backfill_path)
        try:
            content = ''
            if blob.exists():
                content = blob.download_as_text().strip()
                if content:
                    content += f'\n{self.checkpoint_type}_BACKFILL = {current_year},{current_month} (NOT UPLOADED)'
                else:  # for empty file
                    content = f'{self.checkpoint_type}_BACKFILL = {current_year},{current_month} (NOT UPLOADED)'
            else:  # for file not found
                content = f'{self.checkpoint_type}_BACKFILL = {current_year},{current_month} (NOT UPLOADED)'
            blob.upload_from_string(content)
        except Exception as e:
            raise e

