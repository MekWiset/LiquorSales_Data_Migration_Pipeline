from pyspark.sql import SparkSession
from utils.normalizer import Normalizer
from utils.configparser import ConfigParser


spark = SparkSession.builder.appName('Item Number Bridge').master('local[*]').getOrCreate()
norm = Normalizer(spark)

config_path = 'config.yaml'
conf = ConfigParser(config_path)


try:

    # Load Data into Dataframes
    data_files = [
        ('liquor_sales', 'csv')
    ]
    data_frames = norm.load_data(data_files)

    df_liquor_sales = data_frames.get('liquor_sales')

    columns = ['Item Number', 'Item Description', 'Vendor Number', 'Vendor Name']
    df_bridge = df_liquor_sales.select(columns)


    # Data Preprocessing
    try:
        to_remove = ['x']
        df_bridge = norm.remove_string(df_bridge, column='Item Number', to_remove=to_remove)

        df_bridge = norm.upper(df_bridge, column='Vendor Number')
        df_bridge = norm.replace_value_when(df_bridge, column='Vendor Number', condition=df_bridge['Vendor Name'] == 'RESERVOIR DISTILLERY', replacement=988)
        df_bridge = df_bridge.drop('Vendor Name')
    except Exception as e:
        print (f'Error raised at Data Preprocessing section: {e}')


    # Item Description Cleaning
    try:
        to_remove = ['\.', '"', "'", '�', '°']
        df_bridge = norm.remove_string(df_bridge, column='Item Description', to_remove=to_remove)
        df_bridge = norm.initcap(df_bridge, 'Item Description')
    except Exception as e:
        print (f'Error raised at Item Description Cleaning section: {e}')


    # Mode Item Description
    # 1 Item Number = 1 mode(Item Description)
    try:
        mode_description_groupby_cols = ['Item Number']
        df_description_mapping = norm.mode_column(df_bridge, column_to_mode='Item Description', groupby_cols=mode_description_groupby_cols, alias='Current Item Description')

        df_bridge = df_bridge.join(df_description_mapping, on='Item Number', how='left')
        df_bridge = df_bridge.withColumnRenamed('Item Number', 'Original Item Number')
        df_bridge = df_bridge.drop('Item Description')
        df_bridge = df_bridge.dropDuplicates()
    except Exception as e:
        print (f'Error raised at Mode Item Description section: {e}')


    # Mode Item Number
    # 1 Item Description = 1 mode(Item Number)
    try:
        mode_number_groupby_cols = ['Current Item Description', 'Vendor Number']
        df_number_mapping = norm.mode_column(df_bridge, column_to_mode='Original Item Number', groupby_cols=mode_number_groupby_cols, alias='Current Item Number')
        
        df_bridge = df_bridge.join(df_number_mapping, on=['Current Item Description', 'Vendor Number'], how='left')
        df_bridge = df_bridge.drop('Current Item Description')
        df_bridge = df_bridge.drop('Vendor Number')
        df_bridge = df_bridge.dropDuplicates()
    except Exception as e:  
        print (f'Error raised at Mode Item Number section: {e}')


    # Finalizing
    try:
        df_bridge = norm.cast(df_bridge, column='Original Item Number', type='int')
        df_bridge = norm.cast(df_bridge, column='Current Item Number', type='int')

        df_bridge = df_bridge.filter(df_bridge['Original Item Number'] != df_bridge['Current Item Number'])
        df_bridge = df_bridge.orderBy(df_bridge['Current Item Number'], df_bridge['Original Item Number'])
    except Exception as e:
        print (f'Error raised at Finalizing section: {e}')


    # Export Processed Data in '.parquet'
    try:
        output_table = 'item_number_bridge'
        norm.write_data(df_to_export=df_bridge, output_path=conf.get_path(output_table))
    except Exception as e:
        print (f'Error raised at Export Processed Data section: {e}')

    # df_bridge.show()

except Exception as e:
    print (f'An unexpected error occurred: {e}')