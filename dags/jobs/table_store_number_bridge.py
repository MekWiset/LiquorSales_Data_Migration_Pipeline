from pyspark.sql import SparkSession
from utils.normalizer import Normalizer
from utils.configparser import ConfigParser



spark = SparkSession.builder.appName('Store Number Bridge').master('local[*]').getOrCreate()
norm = Normalizer(spark)

config_path = 'config.yaml'
conf = ConfigParser(config_path)


try:

    # Load Data into Dataframes
    data_files = [
        ('liquor_sales', 'csv'),
        ('store_address_bridge', 'parquet')
    ]
    data_frames = norm.load_data(data_files)

    df_liquor_sales = data_frames.get('liquor_sales')
    df_address_bridge = data_frames.get('store_address_bridge')

    columns = ['Store Number', 'Store Name', 'Address']
    df_bridge = df_liquor_sales.select(columns)
    df_bridge = df_bridge.dropDuplicates()


    # Store Name Cleaning
    try:
        to_remove = ['"', "'", '.', ',']
        for item in to_remove:
            df_bridge = norm.replace_string(df_bridge, column='Store Name', string=f'\\{item}', replacement='')
        
        df_bridge = norm.remove_double_spaces(df_bridge, column='Store Name')
        df_bridge = norm.replace_string(df_bridge, column='Store Name', string='&', replacement='and')
        df_bridge = norm.trim(df_bridge, column='Store Name')
        df_bridge = norm.initcap(df_bridge, column='Store Name')
    except Exception as e:
        print (f'Error raised at Store Name Cleaning section: {e}')


    # Mode Store Name
    # 1 Store Number = 1 mode(Store Name)
    try:
        name_groupby_cols = ['Store Number']
        df_name_mapping = norm.mode_column(df_bridge, column_to_mode='Store Name', groupby_cols=name_groupby_cols, alias='Current Store Name')

        df_bridge = df_bridge.join(df_name_mapping, on='Store Number', how='left')
        df_bridge = df_bridge.withColumnRenamed('Store Number', 'Original Store Number')
        df_bridge = df_bridge.drop('Store Name')
        df_bridge = df_bridge.dropDuplicates()
    except Exception as e:
        print (f'Error raised at Mode Store Name section: {e}')
        

    # Store Address Bridge Mapping
    try:
        df_bridge = norm.upper(df_bridge, column='Address')
        df_bridge = df_bridge.join(df_address_bridge, on = df_bridge['Address'] == df_address_bridge['Original Address'], how='left')
        df_bridge = df_bridge.drop('Address').drop('Original Address')
        df_bridge = df_bridge.withColumnRenamed('Current Address', 'Address')
    except Exception as e:
        print (f'Error raised at Store Address Bridge Mapping section: {e}')
        

    # Mode Store Number
    # 1 Store Name = 1 mode(Store Number)
    try:
        number_groupby_cols = ['Current Store Name', 'Address']
        df_number_mapping = norm.mode_column(df_bridge, column_to_mode='Original Store Number', groupby_cols=number_groupby_cols, alias='Current Store Number')

        df_bridge = df_bridge.join(df_number_mapping, on=['Current Store Name', 'Address'], how='left')
        df_bridge = df_bridge.drop('Current Store Name')
        df_bridge = df_bridge.drop('Address')
        df_bridge = df_bridge.dropDuplicates()
    except Exception as e:
        print (f'Error raised at Mode Store Number section: {e}')
        

    # Finalizing
    try:
        df_bridge = df_bridge.filter(df_bridge['Original Store Number'] != df_bridge['Current Store Number'])
        df_bridge = df_bridge.orderBy(df_bridge['Current Store Number'], df_bridge['Original Store Number'])
    except Exception as e:
        print (f'Error raised at Finalizing section: {e}')
        

    # Export Processed Data in '.parquet'
    try:
        output_table='store_number_bridge'
        norm.write_data(df_to_export=df_bridge, output_path=conf.get_path(output_table))
    except Exception as e:
        print (f'Error raised at Export Processed Data section: {e}')
        
    # df_bridge.show()

except Exception as e:
    print (f'An unexpected error occurred: {e}')

    