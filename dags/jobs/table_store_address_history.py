from pyspark.sql import SparkSession
from utils.normalizer import Normalizer
from utils.configparser import ConfigParser


spark = SparkSession.builder.appName('Store Address History').master('local[*]').getOrCreate()
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

    columns = ['Store Number', 'Address', 'City', 'Zip Code', 'County Number', 'Store Location', 'Date']
    df_address_history = df_liquor_sales.select(columns)
    df_address_history = df_address_history.dropDuplicates()
    df_address_history = df_address_history.na.drop(subset=['Address', 'City', 'Zip Code', 'County Number'], how='all')


    # Data Preprocessing
    try:
        to_remove = ['"', "'", '.', ',']
        for item in to_remove:
            df_address_history = norm.replace_string(df_address_history, column='City', string=f'\\{item}', replacement='')

        df_address_history = norm.remove_double_spaces(df_address_history, column='City')
        df_address_history = norm.replace_string(df_address_history, column='City', string='&', replacement='and')
        df_address_history = norm.trim(df_address_history, column='City')
        df_address_history = norm.upper(df_address_history, column='City')

        df_address_history = norm.cast(df_address_history, column='Date', type='date')
        df_address_history = norm.upper(df_address_history, column='Address')
    except Exception as e:
        print (f'Error raised at Data Preprocessing section: {e}')


    # Zip Code Format Correction
    try:
        format_corrections = conf.get_data_correction(column='zip_code', correction_type='string_format_corrections')

        for original, replacement in format_corrections.items():
            df_address_history = norm.replace_value_when(df_address_history, column='Zip Code', condition = df_address_history['Zip Code'] == original, replacement=replacement)
        
        df_address_history = norm.cast(df_address_history, column='Zip Code', type='int')
    except Exception as e:
        print (f'Error raised at Zip Code Format Correction section: {e}')


    # Store Number Bridge Mapping
    try:
        df_address_history = df_address_history.join(df_number_bridge, on = df_address_history['Store Number'] == df_number_bridge['Original Store Number'], how='left')
        df_address_history = norm.replace_value_coalesce(df_address_history, column='Store Number', replacement_column='Current Store Number')
        df_address_history = df_address_history.drop('Original Store Number').drop('Current Store Number')
    except Exception as e:
        print (f'Error raised at Store Number Bridge Mapping section: {e}')


    # Remove NULL Duplicates
    # Rows with duplicates, one with [County Number], one without
    try:
        partition_cols = ['Store Number', 'Address', 'City', 'Zip Code'],
        df_address_history = norm.decide_null_rows(df_address_history, column_to_decide='County Number', partition_cols=partition_cols)
    except Exception as e:
        print (f'Error raised at Remove NULL Duplicates section: {e}')


    # Mode Store Location
    try:
        location_groupby_cols = ['Store Number', 'Address', 'City', 'Zip Code', 'County Number']
        df_mode_location = norm.mode_column(df_address_history, column_to_mode='Store Location', groupby_cols=location_groupby_cols, alias='Store Location')
        df_address_history = df_address_history.join(df_mode_location, on=['Store Number', 'Address', 'City', 'Zip Code', 'County Number','Store Location'], how='left')
    except Exception as e:
        print (f'Error raised at Mode Store Location section: {e}')


    # From Date / To Date
    try:
        groupby_cols = ['Store Number', 'Address', 'City', 'Zip Code', 'County Number', 'Store Location']
        df_address_history = norm.create_date_range(df_address_history, groupby_cols=groupby_cols, min_date_col='Date', max_date_col='Date')
    except Exception as e:
        print (f'Error raised at From Date / To Date section: {e}')


    # Apply County Number Corrections
    try:
        county_number_mappings = conf.get_data_correction(column='county_number', correction_type='store_county_number_mappings')
        
        for store_number, county_number in county_number_mappings.items():
            df_address_history = norm.replace_value_when(df_address_history, column='County Number', condition=df_address_history['Store Number'] == store_number, replacement=county_number)
    except Exception as e:
        print (f'Error raised at Apply County Number Corrections section: {e}')


    # Apply Zip Code Corrections
    try:
        zip_code_mappings = conf.get_data_correction(column='zip_code', correction_type='store_zipcode_mappings')
        
        for store_number, zip_code in zip_code_mappings.items():
            df_address_history = norm.replace_value_when(df_address_history, column='Zip Code', condition=df_address_history['Store Number'] == store_number, replacement=zip_code)
    except Exception as e:
        print (f'Error raised at Apply Zip Code Corrections section: {e}')


    # Apply City Corrections
    try:
        city_mappings = conf.get_data_correction(column='city', correction_type='store_city_mappings')
        for store_number, city in city_mappings.items():
            df_address_history = norm.replace_value_when(df_address_history, column='City', condition = df_address_history['Store Number'] == store_number, replacement=city)

        city_corrections = conf.get_data_correction(column='city', correction_type='corrections')
        for original ,correction in city_corrections.items():
            df_address_history = norm.replace_value_when(df_address_history, column='City', condition=df_address_history['City'] == original, replacement=correction)
    except Exception as e:
        print (f'Error raised at Apply City Corrections section: {e}')


    # Address Corrections
    try:
        df_address_history = df_address_history.join(df_address_bridge, on = df_address_history['Address'] == df_address_bridge['Original Address'])
        df_address_history = df_address_history.drop('Address').drop('Original Address')
        df_address_history = df_address_history.withColumnRenamed('Current Address', 'Address')
    except Exception as e:
        print (f'Error raised at Address Corrections section: {e}')


    # Split Store Location Column
    # Split [Store Location] into [Latitude and Longitude]
    try:
        df_address_history = norm.split_column(df_address_history, column_to_split='Store Location', index=1, new_column_name='Longitude')
        df_address_history = norm.split_column(df_address_history, column_to_split='Store Location', index=2, new_column_name='Latitude')
        df_address_history = df_address_history.drop('Store Location')

    except Exception as e:
        print (f'Error raised at Split Store Location Column section: {e}')


    df_date = df_address_history.select(['Store Number', 'Address', 'City', 'Zip Code', 'County Number', 'From Date', 'To Date'])  # Split 'From Date' and 'To Date' column to join later


    # Mode Latitude and Longitude
    try:
        lat_long_groupby_cols = ['Store Number', 'Address', 'City', 'Zip Code', 'County Number']
        lat_long_columns_to_mode = ['Latitude', 'Longitude']
        lat_long_aliases = ['Latitude', 'Longitude']
        df_address_history = norm.mode_multiple_columns(df_address_history, columns_to_mode=lat_long_columns_to_mode, groupby_cols=lat_long_groupby_cols, aliases=lat_long_aliases)
    except Exception as e:
        print (f'Error raised at Mode Latitude and Longitude section: {e}')


    # Date Column
    try:
        df_address_history = df_address_history.join(df_date, on=['Store Number', 'Address', 'City', 'Zip Code', 'County Number'], how='left')
    except Exception as e:
        print (f'Error raised at Date Column section: {e}')


    # Join Date Range 
    # From [From Date] to [To Date]
    try:
        join_groupby_cols = ['Store Number', 'Address', 'City', 'Zip Code', 'County Number', 'Latitude', 'Longitude']
        df_address_history = norm.create_date_range(df_address_history, groupby_cols=join_groupby_cols, min_date_col='From Date', max_date_col='To Date')
    except Exception as e:
        print (f'Error raised at Join Date Range section: {e}')


    # Is Current Column -> most recent
    try:
        recent_partition_cols = ['Store Number']
        df_address_history = norm.most_recent_True(df_address_history, partition_cols=recent_partition_cols, order_by_col='To Date', new_column_name='Is Current')
    except Exception as e:
        print (f'Error raised at Is Current Column section: {e}')


    # Sequence of Occurrence
    try:
        sequence_partition_cols = ['Store Number']
        df_address_history = norm.occurrence(df_address_history, partition_cols=sequence_partition_cols, order_by_col='To Date', new_column_name='Address Sequence')
    except Exception as e:
        print (f'Error raised at Sequence of Occurrence section: {e}')


    # Finalizing
    try:
        column_order = ['Store Number', 'Address', 'City', 'Zip Code', 'County Number', 'Latitude', 'Longitude', 'From Date', 'To Date', 'Is Current', 'Address Sequence']
        df_address_history = df_address_history.select(column_order)

        df_address_history = norm.add_prefix(df_address_history, column='County Number', prefix='CNT_')
        df_address_history = norm.add_prefix(df_address_history, column='Store Number', prefix='STO_')
        df_address_history = norm.cast(df_address_history, column='Zip Code', type='int')

        df_address_history = df_address_history.orderBy(df_address_history['Store Number'], df_address_history['To Date'])
    except Exception as e:
        print (f'Error raised at Finalizing section: {e}')


    # Export Processed Data in '.parquet'
    try:
        output_table = 'store_address_history'
        norm.write_data(df_to_export=df_address_history, output_path=conf.get_path(output_table))
    except Exception as e:
        print (f'Error raised at Export Processed Data section: {e}')

    # df_address_history.show()

except Exception as e:
    print (f'An unexpected error occurred: {e}')

    