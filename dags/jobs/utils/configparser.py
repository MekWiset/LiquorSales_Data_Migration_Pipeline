from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, DateType, BooleanType
import yaml


class ConfigParser:

    '''
    Parses configuration data from a YAML file.

    Args:
        config_path (str): The path to the YAML configuration file.

    Methods:
        load_config(self) -> None:
            Loads configuration data from the YAML file.
        
        get_path(self, file_name: str) -> str:
            Retrieves the file path from the configuration for a given file name.
        
        get_schema(self, file_name: str) -> StructType:
            Retrieves the schema for a given file name from the configuration.

        def get_checkpoint_type(self, checkpoint_file: str) -> str:
            Retrieves the checkpoint type from the configuration for a checkpoint file name.

        get_data_correction(self, column: str, correction_type: str) -> dict:
            Retrieves data correction information for a specified column and correction type.
        
        gcp_credentials_path(self, role: str) -> str:
            Retrieves the GCP credentials path for a given role from the configuration.
        
        bucket_info(self, which_bucket: str) -> tuple[str, str]:
            Retrieves the name and location of a GCP bucket from the configuration.
        
        dir_table_pair(self, destination_table: str) -> tuple[str, str]:
            Retrieves the GCS directory and BigQuery table ID pair for a destination table.
            
    Attributes:
        config (str): The path to the YAML configuration file.
        config_data (dict): The loaded configuration data from the YAML file.
    '''

    def __init__(self, config_path: str) -> None:
        self.config = config_path
        self.load_config()


    def load_config(self) -> None:
        '''Loads configuration data from the YAML file.'''
        with open(self.config, 'r') as file:
            self.config_data = yaml.safe_load(file)


    def get_path(self, file_name: str) -> str:
        '''Retrieves the file path from the configuration for a given file name.'''
        return self.config_data['files'][file_name]['path']
    

    def get_schema(self, file_name: str) -> StructType:
        '''Retrieves the schema for a given file name from the configuration.'''
        schema = StructType()
        for field in self.config_data['files'][file_name]['schema']:
            schema.add(field['column_name'], eval(field['data_type']), field['nullable'])
        return schema
    

    def get_data_correction(self, column: str, correction_type: str) -> dict:
        '''Retrieves data correction information for a specified column and correction type.'''
        return self.config_data['data_corrections'][column][correction_type]


    def gcp_credentials_path(self, role: str) -> str:
        '''Retrieves the GCP credentials path for a given role from the configuration.'''
        return self.config_data['GCP']['roles'][role]['credentials_path']


    def bucket_info(self, which_bucket: str) -> tuple[str, str]:
        '''Retrieves the name and location of a GCP bucket from the configuration.'''
        buckets = self.config_data['GCP']['buckets'][which_bucket]
        for bucket_data in buckets:
            bucket_name = bucket_data.get('name')
            bucket_location = bucket_data.get('location')
            return bucket_name, bucket_location


    def dir_table_pair(self, destination_table: str) -> tuple[str, str]:
        '''Retrieves the GCS directory and BigQuery table ID pair for a destination table.'''
        tables = self.config_data['GCP']['tables'][destination_table]
        for table_data in tables:
            gcs_directory = table_data.get('gcs_directory')
            bq_table_id = table_data.get('bq_table_id')
            return gcs_directory, bq_table_id

