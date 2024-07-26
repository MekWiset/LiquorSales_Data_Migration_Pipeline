from pyspark.sql import SparkSession
from pyspark.sql.functions import round
from utils.normalizer import Normalizer
from utils.configparser import ConfigParser
from utils.checkpointmanager import CheckpointManager


spark = SparkSession.builder.appName('Invoice').master('local[*]').getOrCreate()
norm = Normalizer(spark)

config_path = 'config.yaml'
conf = ConfigParser(config_path)

credentials_path = 'credentials/tt-dev-001-419206-4551615b3e2a.json'
bucket_name = 'liquor_sales_bucket'
checkpoint_path = 'checkpoints/invoice_checkpoint.txt'
checkpoint_type = 'invoice'
cp = CheckpointManager(credentials_path, bucket_name, checkpoint_path, checkpoint_type)


try:

    # Load Data into Dataframes
    data_files = [
        ('item_number_bridge', 'parquet'),
        ('item_price_history', 'parquet'),
        ('store_number_bridge', 'parquet'),
        ('store_address_bridge', 'parquet'),
        ('store_address_history', 'parquet')
    ]
    data_frames = norm.load_data(data_files)

    df_item_number_bridge = data_frames.get('item_number_bridge')
    df_price_history = data_frames.get('item_price_history')
    df_store_number_bridge = data_frames.get('store_number_bridge')
    df_address_bridge = data_frames.get('store_address_bridge')
    df_address_history = data_frames.get('store_address_history')


    # Load partitioned liquor_sales data
    current_year, current_month = cp.get_current_partition()
    partition_path = f'datasets/liquor_sales_partition/Year={current_year}/Month={current_month}'
    liquor_sales_schema = conf.get_schema('liquor_sales')

    df_liquor_sales = norm.read_data(partition_path, schema=liquor_sales_schema)

    columns = ['Invoice/Item Number', 'Store Number', 'Address', 'Item Number', 'Vendor Number', 'Vendor Name', 'Pack', 'Bottle Volume (ml)', 'State Bottle Cost', 'State Bottle Retail', 'Bottles Sold']
    df_invoice = df_liquor_sales.select(columns)
    df_invoice = df_invoice.dropDuplicates()


    # Data Preprocessing
    try:
        to_remove = ['x']
        df_invoice = norm.remove_string(df_invoice, column='Item Number', to_remove=to_remove)
        df_invoice = df_invoice.withColumnRenamed('Invoice/Item Number', 'Invoice_Item Number')
        df_invoice = df_invoice.withColumnRenamed('Bottles Sold', 'Items Sold')
        df_invoice = df_invoice.withColumnRenamed('Bottle Volume (ml)', 'Bottle Volume ml')
    except Exception as e:
        print (f'Error raised at Data Preprocessing section: {e}')


    # Item Number Bridge Mapping
    try:
        df_invoice = df_invoice.join(df_item_number_bridge, on = df_invoice['Item Number'] == df_item_number_bridge['Original Item Number'], how='left')
        df_invoice = norm.replace_value_coalesce(df_invoice, column='Item Number', replacement_column='Current Item Number')
        df_invoice = df_invoice.drop('Original Item Number').drop('Current Item Number')
        df_invoice = norm.add_prefix(df_invoice, column='Item Number', prefix='ITM_')
    except Exception as e:
        print (f'Error raised at Item Number Bridge Mapping section: {e}')


    # Price Sequence Occurrence
    try:
        df_invoice = norm.replace_value_when(df_invoice, column='Vendor Number', condition=df_invoice['Vendor Name'] == 'Reservoir Distillery', replacement=988)
        df_invoice = norm.add_prefix(df_invoice, column='Vendor Number', prefix='VEN_')
        df_invoice = df_invoice.join(df_price_history, on=['Item Number', 'Vendor Number', 'Pack', 'Bottle Volume ml', 'State Bottle Cost', 'State Bottle Retail'], how='left')
    except Exception as e:
        print (f'Error raised at Price Sequence Occurrence section: {e}')
        

    # Sale Dollars Column
    try:
        df_invoice = df_invoice.withColumn('Sale Dollars', round(df_invoice['State Bottle Retail'] * df_invoice['Items Sold'], 2))
    except Exception as e:
        print (f'Error raised at Sale Dollars Column section: {e}')
        

    # Sale Baht Column
    try:
        conversion_rate = 36
        df_invoice = df_invoice.withColumn('Sale Baht', round(df_invoice['Sale Dollars'] * conversion_rate, 2))
    except Exception as e:
        print (f'Error raised at Sale Baht Column section: {e}')
        

    # Volume Sold Liters Column
    try:
        df_invoice = df_invoice.withColumn('Volume Sold Liters', (df_invoice['Bottle Volume ml'] * df_invoice['Items Sold']) / 1000)
    except Exception as e:
        print (f'Error raised at Volume Sole Liters Column section: {e}')
        

    column_order = ['Invoice_Item Number', 'Store Number', 'Address', 'Item Number', 'Vendor Number', 'Price Sequence', 'Items Sold', 'Sale Dollars', 'Sale Baht', 'Volume Sold Liters']
    df_invoice = df_invoice.select(column_order)
    df_invoice = df_invoice.dropDuplicates()


    # Store Number Bridge Mapping
    try:
        df_invoice = df_invoice.join(df_store_number_bridge, on = df_invoice['Store Number'] == df_store_number_bridge['Original Store Number'], how='left')
        df_invoice = norm.replace_value_coalesce(df_invoice, column='Store Number', replacement_column='Current Store Number')

        df_invoice = df_invoice.drop('Original Store Number').drop('Current Store Number')
        df_invoice = norm.add_prefix(df_invoice, column='Store Number', prefix='STO_')
    except Exception as e:
        print (f'Error raised at Store Number Bridge Mapping section: {e}')
        

    # Address Sequence Occurrence
    try:
        df_invoice = norm.upper(df_invoice, column='Address')
        df_invoice = df_invoice.join(df_address_bridge, on = df_invoice['Address'] == df_address_bridge['Original Address'])
        df_invoice = df_invoice.drop('Address').drop('Original Address')
        df_invoice = df_invoice.withColumnRenamed('Current Address', 'Address')
    except Exception as e:
        print (f'Error raised at Address Sequence Occurrence section: {e}')
        

    df_invoice = df_invoice.join(df_address_history, on=['Store Number', 'Address'], how='left')


    # Date Column
    try:
        columns = ['Invoice/Item Number', 'Date']
        df_liquor_sales_date = df_liquor_sales.select(columns)
        df_liquor_sales_date = df_liquor_sales_date.withColumnRenamed('Invoice/Item Number', 'Invoice_Item Number')

        df_liquor_sales_date = norm.cast(df_liquor_sales_date, column='Date', type='date')
        df_invoice = df_invoice.join(df_liquor_sales_date, on='Invoice_Item Number', how='left')
    except Exception as e:
        print (f'Error raised at Date Column section: {e}')
        

    # Finalizing
    try:
        column_order = ['Invoice_Item Number', 'Store Number', 'Address Sequence', 'Item Number', 'Vendor Number', 'Price Sequence', 'Items Sold', 'Sale Dollars', 'Sale Baht', 'Volume Sold Liters', 'Date']
        df_invoice = df_invoice.select(column_order)
        df_invoice = df_invoice.dropDuplicates()

        df_invoice = df_invoice.orderBy('Invoice_Item Number')
    except Exception as e:
        print (f'Error raised at Finalizing section: {e}')
        

    # Export Processed Data in '.parquet'
    try:
        output_path = f'outputs/invoice_partitions/Year={current_year}/Month={current_month}'
        norm.write_data(df_to_export=df_invoice, output_path=output_path, num_partitions=1)

        # Mark Checkpoint in checkpoint.py
        cp.update_checkpoint(current_year, current_month, status='File Created')
    except Exception as e:
        print (f'Error raised at Export Processed Data section: {e}')
    

except Exception as e:
    print (f'An unexpected error occurred: {e}')

