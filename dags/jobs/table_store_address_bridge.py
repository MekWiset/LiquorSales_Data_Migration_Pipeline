from pyspark.sql import SparkSession
from utils.normalizer import Normalizer
from utils.configparser import ConfigParser


spark = SparkSession.builder.appName('Store Address Bridge').master('local[*]').getOrCreate()
norm = Normalizer(spark)

config_path = 'config.yaml'
conf = ConfigParser(config_path)


try:

    # Load Data into Dataframes
    data_files = [
        ('liquor_sales', 'csv')
    ]
    data_frames = norm.load_data(data_files)

    df_liquor_sales = data_frames['liquor_sales']

    columns = ['Address']
    df_bridge = df_liquor_sales.select(columns)
    df_bridge = df_bridge.dropDuplicates()


    # Original Address Column
    try:
        df_bridge = df_bridge.withColumn('Original Address', df_bridge['Address'])
        df_bridge = norm.upper(df_bridge, column='Original Address')
    except Exception as e:
        print (f'Error raised at Original Address Column section: {e}')


    # Current Address Cleaning
    try:
        df_bridge = df_bridge.withColumnRenamed('Address', 'Current Address')

        to_remove = ['"', "'", '.', ',']
        for item in to_remove:
            df_bridge = norm.replace_string(df_bridge, column='Current Address', string=f'\\{item}', replacement='')

        df_bridge = norm.remove_double_spaces(df_bridge, column='Current Address')
        df_bridge = norm.replace_string(df_bridge, column='Current Address', string='&', replacement='and')
        df_bridge = norm.trim(df_bridge, column='Current Address')
        df_bridge = norm.upper(df_bridge, column='Current Address')
    except Exception as e:
        print (f'Error raised at Current Address Cleaning section: {e}')


    # Apply Abbreviation Corrections
    try:
        abbreviation_corrections = {
            r'\bHWY\b': 'HIGHWAY',
            r'\bDR\b': 'DRIVE',
            r'\bAVE\b': 'AVENUE',
            r'\bST\b': 'STREET',
            r'\bRD\b': 'ROAD',
            r'\bN\b': 'NORTH',
            r'\bE\b': 'EAST',
            r'\bS\b': 'SOUTH',
            r'\bW\b': 'WEST',
            r'\bNE\b': 'NORTHEAST',
            r'\bNW\b': 'NORTHWEST',
            r'\bSE\b': 'SOUTHEAST',
            r'\bSW\b': 'SOUTHWEST',
            r'\bSTREET\b': '',
            r'PO B.*': ''
        }
        for abbreviation, correction in abbreviation_corrections.items():
            df_bridge = norm.replace_string(df_bridge, column='Current Address', string=abbreviation, replacement=correction)

        df_bridge = norm.remove_double_spaces(df_bridge, column='Current Address')
        df_bridge = norm.trim(df_bridge, column='Current Address')
    except Exception as e:
        print (f'Error raised at Apply Abbreviation Corrections section: {e}')


    # Apply Address Corrections
    try:
        address_corrections = conf.get_data_correction(column='address', correction_type='corrections')

        for original, correction in address_corrections.items():
            df_bridge = norm.replace_value_when(df_bridge, column='Current Address', condition=df_bridge['Current Address'] == original, replacement=correction)
    except Exception as e:
        print (f'Error raised at Apply Address Corrections section: {e}')


    # Finalizing
    try:
        column_order = ['Original Address', 'Current Address']
        df_bridge = df_bridge.select(column_order)

        df_bridge = df_bridge.dropDuplicates()
        df_bridge = df_bridge.orderBy(df_bridge['Original Address'])
    except Exception as e:
        print (f'Error raised at Finalizing section: {e}')


    # Export Processed Data in '.parquet'
    try:
        output_table = 'store_address_bridge'
        norm.write_data(df_to_export=df_bridge, output_path=conf.get_path(output_table))
    except Exception as e:
        print (f'Error raised at Export Processed Data section: {e}')

        # df_bridge.show()

except Exception as e:
    print (f'An unexpected error occurred: {e}')

    