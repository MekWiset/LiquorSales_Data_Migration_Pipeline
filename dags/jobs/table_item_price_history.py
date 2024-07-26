from pyspark.sql import SparkSession
from utils.normalizer import Normalizer
from utils.configparser import ConfigParser


spark = SparkSession.builder.appName('Item Price History').master('local[*]').getOrCreate()
norm = Normalizer(spark)

config_path = 'config.yaml'
conf = ConfigParser(config_path)


try:

    # Load Data into Dataframes
    data_files = [
        ('liquor_sales', 'csv'),
        ('item_number_bridge', 'parquet')
    ]
    data_frames = norm.load_data(data_files)

    df_liquor_sales = data_frames.get('liquor_sales')
    df_item_number_bridge = data_frames.get('item_number_bridge')

    columns = ['Item Number', 'Vendor Number', 'Vendor Name', 'Pack', 'Bottle Volume (ml)', 'State Bottle Cost', 'State Bottle Retail', 'Date']
    df_price_history = df_liquor_sales.select(columns)


    # Data Preprocessing
    try:
        to_remove = ['x']
        df_price_history = norm.remove_string(df_price_history, column='Item Number', to_remove=to_remove)

        df_price_history = norm.cast(df_price_history, column='Date', type='date', date_format='MM/dd/yyyy')
        df_price_history = df_price_history.withColumnRenamed('Bottle Volume (ml)', 'Bottle Volume ml')

        df_price_history = norm.replace_value_when(df_price_history, column='Vendor Number', condition=df_price_history['Vendor Name'] == 'Reservoir Distillery', replacement=988)
        df_price_history = norm.add_prefix(df_price_history, column='Vendor Number', prefix='VEN_')
        df_price_history = df_price_history.drop('Vendor Name')
    except Exception as e:
        print (f'Error raised at Data Preprocessing section: {e}')


    # Item Number Bridge Mapping
    try:
        df_price_history = df_price_history.join(df_item_number_bridge, on = df_price_history['Item Number'] == df_item_number_bridge['Original Item Number'], how='left')
        df_price_history = norm.replace_value_coalesce(df_price_history, column='Item Number', replacement_column='Current Item Number')
        df_price_history = df_price_history.drop('Original Item Number').drop('Current Item Number')
    except Exception as e:
        print (f'Error raised at Item Number Bridge Mapping section: {e}')


    # From Date / To Date
    try:
        groupby_cols = ['Item Number', 'Vendor Number', 'Pack', 'Bottle Volume ml', 'State Bottle Cost', 'State Bottle Retail']
        df_price_history = norm.create_date_range(df_price_history, groupby_cols=groupby_cols, min_date_col='Date', max_date_col='Date')
    except Exception as e:
        print (f'Error raised at From Date / To Date section: {e}')


    # Is Current Column -> most recent
    try:
        recent_partition_cols = ['Item Number', 'Vendor Number']
        df_price_history = norm.most_recent_True(df_price_history, partition_cols=recent_partition_cols, order_by_col='To Date', new_column_name='Is Current')
    except Exception as e:
        print (f'Error raised at Is Current Column section: {e}')


    # Sequence of Occurrence
    try:
        sequence_partition_cols = ['Item Number', 'Vendor Number']
        df_price_history = norm.occurrence(df_price_history, partition_cols=sequence_partition_cols, order_by_col='To Date', new_column_name='Price Sequence')
    except Exception as e:
        print (f'Error raised at Sequence of Occurrence section: {e}')


    # Finalizing
    try:
        df_price_history = norm.add_prefix(df_price_history, column='Item Number', prefix='ITM_')
        df_price_history = df_price_history.orderBy(df_price_history['Item Number'], df_price_history['Vendor Number'], df_price_history['To Date'])
    except Exception as e:
        print (f'Error raised at Finalizing section: {e}')


    # Export Processed Data in '.parquet'
    try:
        output_table = 'item_price_history'
        norm.write_data(df_to_export=df_price_history, output_path=conf.get_path(output_table))
    except Exception as e:
        print (f'Error raised at Export Processed Data section: {e}')

    # df_price_history.show()

except Exception as e:
    print (f'An unexpected error occurred: {e}')

    