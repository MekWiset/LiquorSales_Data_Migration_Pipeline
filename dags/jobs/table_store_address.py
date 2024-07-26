from pyspark.sql import SparkSession
from utils.normalizer import Normalizer
from utils.configparser import ConfigParser


spark = SparkSession.builder.appName('Store Address').master('local[*]').getOrCreate()
norm = Normalizer(spark)

config_path = 'config.yaml'
conf = ConfigParser(config_path)


try:

    # Load Data into Dataframes
    data_files = [
        ('liquor_sales', 'csv'),
        ('store_number_bridge', 'parquet'),
        ('store_address_bridge', 'parquet')
    ]
    data_frames = norm.load_data(data_files)

    df_liquor_sales = data_frames.get('liquor_sales')
    df_number_bridge = data_frames.get('store_number_bridge')
    df_address_bridge = data_frames.get('store_address_bridge')

    columns = ['Store Number', 'Store Name', 'Address', 'City', 'Zip Code', 'County Number', 'Store Location', 'Date']
    df_addresses = df_liquor_sales.select(columns)
    df_addresses = df_addresses.dropDuplicates()


    # Data Preprocessing
    try:
        columns_to_clean = ['Store Name', 'City']

        for column in columns_to_clean:

            to_remove = ['"', "'", '.', ',']
            for item in to_remove:
                df_addresses = norm.replace_string(df_addresses, column=column, string=f'\\{item}' ,replacement='')

            df_addresses = norm.remove_double_spaces(df_addresses, column=column)
            df_addresses = norm.replace_string(df_addresses, column=column, string='&', replacement='and')
            df_addresses = norm.trim(df_addresses, column=column)

        df_addresses = norm.cast(df_addresses, column='Date', type='date')
        df_addresses = norm.upper(df_addresses, column='Address')
        df_addresses = norm.upper(df_addresses, column='City')
        df_addresses = norm.initcap(df_addresses, column='Store Name')
    except Exception as e:
        print (f'Error raised at Data Preprocessing section: {e}')


    # Zip Code Format Correction
    try:
        format_corrections = conf.get_data_correction(column='zip_code', correction_type='string_format_corrections')
        
        for original, replacement in format_corrections.items():
            df_addresses = norm.replace_value_when(df_addresses, column='Zip Code', condition = df_addresses['Zip Code'] == original, replacement=replacement)
        
        df_addresses = norm.cast(df_addresses, column='Zip Code', type='int')
    except Exception as e:
        print (f'Error raised at Zip Code Format Correction section: {e}')


    # Store Number Bridge Mapping
    try:
        df_addresses = df_addresses.join(df_number_bridge, on = df_addresses['Store Number'] == df_number_bridge['Original Store Number'], how='left')
        df_addresses = norm.replace_value_coalesce(df_addresses, column='Store Number', replacement_column='Current Store Number')
        df_addresses = df_addresses.drop('Original Store Number').drop('Current Store Number')
    except Exception as e:
        print (f'Error raised at Store Number Bridge Mapping section: {e}')
        

    # Remove NULL Duplicates
    # Rows with duplicates, one with [County Number], one without
    try:
        partition_cols = ['Store Number']
        df_addresses = norm.decide_null_rows(df_addresses, column_to_decide='County Number', partition_cols=partition_cols)
    except Exception as e:
        print (f'Error raised at Remove NULL Duplicates section: {e}')
        

    # Apply County Number Corrections
    try:
        county_number_mappings = conf.get_data_correction(column='county_number', correction_type='store_county_number_mappings')
        
        for store_number, county_number in county_number_mappings.items():
            df_addresses = norm.replace_value_when(df_addresses, column='County Number', condition=df_addresses['Store Number'] == store_number, replacement=county_number)
    except Exception as e:
        print (f'Error raised at Apply County Number Corrections section: {e}')
        

    # Apply Zip Code Corrections
    try:
        zip_code_mappings = conf.get_data_correction(column='zip_code', correction_type='store_zipcode_mappings')
        
        for store_number, zip_code in zip_code_mappings.items():
            df_addresses = norm.replace_value_when(df_addresses, column='Zip Code', condition=df_addresses['Store Number'] == store_number, replacement=zip_code)
    except Exception as e:
        print (f'Error raised at Apply Zip Code Corrections section: {e}')
        

    # Apply City Corrections
    try:
        city_mappings = conf.get_data_correction(column='city', correction_type='store_city_mappings')
        for store_number, city in city_mappings.items():
            df_addresses = norm.replace_value_when(df_addresses, column='City', condition = df_addresses['Store Number'] == store_number, replacement=city)

        city_corrections = conf.get_data_correction(column='city', correction_type='corrections')
        for original, correction in city_corrections.items():
            df_addresses = norm.replace_value_when(df_addresses, column='City', condition=df_addresses['City'] == original, replacement=correction)
    except Exception as e:
        print (f'Error raised at Apply City Corrections section: {e}')
        

    # Apply Address Corrections
    try:
        df_addresses = df_addresses.join(df_address_bridge, on = df_addresses['Address'] == df_address_bridge['Original Address'])
        df_addresses = df_addresses.drop('Address').drop('Original Address')
        df_addresses = df_addresses.withColumnRenamed('Current Address', 'Address')
    except Exception as e:
        print (f'Error raised at Apply Address Corrections section: {e}')


    # Mode Store Name
    try:
        store_name_groupby_cols = ['Store Number']
        df_store_name_mapping = norm.mode_column(df_addresses, column_to_mode='Store Name', groupby_cols=store_name_groupby_cols, alias='mode(Store Name)')
        
        df_addresses = df_addresses.join(df_store_name_mapping, on='Store Number', how='left')
        df_addresses = df_addresses.drop('Store Name')
        df_addresses = df_addresses.withColumnRenamed('mode(Store Name)', 'Store Name')
    except Exception as e:
        print (f'Error raised at Mode Store Name section: {e}')
        

    # Split Store Location Column
    # Split [Store Location] into [Latitude and Longitude]
    try:
        df_addresses = norm.split_column(df_addresses, column_to_split='Store Location', index=1, new_column_name='Longitude')
        df_addresses = norm.split_column(df_addresses, column_to_split='Store Location', index=2, new_column_name='Latitude')
        df_addresses = df_addresses.drop('Store Location')
    except Exception as e:
        print (f'Error raised at Split Store Location Column section: {e}')


    df_date = df_addresses.select(['Store Number', 'Store Name', 'Address', 'City', 'Zip Code', 'County Number', 'Date'])


    # Mode Latitude and Longitude
    try:
        lat_long_groupby_cols = ['Store Number', 'Store Name', 'Address', 'City', 'Zip Code', 'County Number']
        lat_long_columns_to_mode = ['Latitude', 'Longitude']
        lat_long_aliases = ['Latitude', 'Longitude']
        df_addresses = norm.mode_multiple_columns(df_addresses, columns_to_mode=lat_long_columns_to_mode, groupby_cols=lat_long_groupby_cols, aliases=lat_long_aliases)
    except Exception as e:
        print (f'Error raised at Mode Latitude and Longitude section: {e}')
        

    # Date Column
    try:
        df_addresses = df_addresses.join(df_date, on=['Store Number', 'Store Name', 'Address', 'City', 'Zip Code', 'County Number'], how='left')
    except Exception as e:
        print (f'Error raised at Date Column section: {e}')
        

    # Keep Latest Row on [Store Number]
    try:
        keep_latest_partition_cols = ['Store Number']
        df_addresses = norm.keep_latest_row(df_addresses, partition_cols=keep_latest_partition_cols, order_by_col='Date')
        df_addresses = df_addresses.drop('Date')
    except Exception as e:
        print (f'Error raised at Keep Latest Row section: {e}')
        


    # Finalizing
    try:
        column_order = ['Store Number', 'Store Name', 'Address', 'City', 'Zip Code', 'County Number', 'Latitude', 'Longitude']
        df_addresses = df_addresses.select(column_order)

        df_addresses = norm.add_prefix(df_addresses, column='County Number', prefix='CNT_')
        df_addresses = norm.add_prefix(df_addresses, column='Store Number', prefix='STO_')

        df_addresses = norm.cast(df_addresses, column='Zip Code', type='int')
        df_addresses = df_addresses.orderBy(df_addresses['Store Number'])
    except Exception as e:
        print (f'Error raised at Finalizing section: {e}')
        

    # Export Processed Data in '.parquet'
    try:
        output_table = conf.get_path('store_address')
        norm.write_data(df_to_export=df_addresses, output_path=output_table)
    except Exception as e:
        print (f'Error raised at Export Processed Data section: {e}')
        
    # df_addresses.show()

except Exception as e:
    print (f'An unexpected error occurred: {e}')

    