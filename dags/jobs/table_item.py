from pyspark.sql import SparkSession
from utils.normalizer import Normalizer
from utils.configparser import ConfigParser


spark = SparkSession.builder.appName('Items').master('local[*]').getOrCreate()
norm = Normalizer(spark)

config_path = 'config.yaml'
conf = ConfigParser(config_path)


try:

    # Load Data into Dataframes
    data_files = [
        ('liquor_sales', 'csv'),
        ('category', 'parquet'),
        ('item_number_bridge', 'parquet')
    ]
    data_frames = norm.load_data(data_files)

    df_liquor_sales = data_frames.get('liquor_sales')
    df_categories = data_frames.get('category')
    df_item_number_bridge = data_frames.get('item_number_bridge')

    columns = ['Item Number', 'Item Description', 'Category Name', 'Vendor Number', 'Vendor Name', 'Pack', 'Bottle Volume (ml)', 'State Bottle Cost', 'State Bottle Retail', 'Date']
    df_items = df_liquor_sales.select(columns)


    # Data Preprocessing
    try:
        to_remove = ['x']
        df_items = norm.remove_string(df_items, column='Item Number', to_remove=to_remove)
        df_items = df_items.withColumnRenamed('Bottle Volume (ml)', 'Bottle Volume ml')
        df_items = norm.initcap(df_items, column='Category Name')
        df_items = norm.remove_double_spaces(df_items, column='Category Name')
        df_items = norm.cast(df_items, column='Date', type='date', date_format='MM/dd/yyyy')
    except Exception as e:
        print (f'Error raised at Data Preprocessing section: {e}')


    # Item Description Cleaning
    try:
        to_remove = ['\.', ''', ''', '�', '°']
        df_items = norm.remove_string(df_items, column='Item Description', to_remove=to_remove)
        df_items = norm.initcap(df_items, 'Item Description')
    except Exception as e:
        print (f'Error raised at Item Description Cleaning section: {e}')


    # Item Number Bridge Mapping
    try:
        df_items = df_items.join(df_item_number_bridge, on = df_items['Item Number'] == df_item_number_bridge['Original Item Number'], how='left')
        df_items = norm.replace_value_coalesce(df_items, column='Item Number', replacement_column='Current Item Number')
        df_items = df_items.drop('Original Item Number').drop('Current Item Number')
    except Exception as e:
        print (f'Error raised at Item Number Brige Mapping section: {e}')


    # Mode Item Description
    # 1 Item Number = 1 mode(Item Description)
    try:
        description_groupby_cols = ['Item Number']
        df_description_moding = norm.mode_column(df_items, column_to_mode='Item Description', groupby_cols=description_groupby_cols, alias='Item Description Map')

        df_items = df_items.join(df_description_moding, on='Item Number', how='left')
        df_items = df_items.drop('Item Description')
        df_items = df_items.withColumnRenamed('Item Description Map', 'Item Description')
    except Exception as e:
        print (f'Error raised at Mode Item Description section: {e}')


    # Mode Category
    # 1 Item Number =  1 mode(Category)
    try:
        category_groupby_cols = ['Item Description']
        df_category_moding = norm.mode_column(df_items, column_to_mode='Category Name', groupby_cols=category_groupby_cols, alias='Category Map')
        df_items = df_items.join(df_category_moding, on='Item Description', how='left')
        df_items = df_items.drop('Category Name')
        df_items = df_items.withColumnRenamed('Category Map', 'Category Name')
    except Exception as e:
        print (f'Error raised at Mode Category section: {e}')


    # Vendor Column
    try:
        df_items = norm.replace_value_when(df_items, column='Vendor Number', condition=df_items['Vendor Name'] == 'RESERVOIR DISTILLERY', replacement=988)
        df_items = norm.add_prefix(df_items, column='Vendor Number', prefix='VEN_')
        df_items = df_items.drop('Vendor Name')
    except Exception as e:
        print (f'Error raised at Vendor Column section: {e}')


    # Apply Category Name Corrections
    try:
        category_name_corrections = conf.get_data_correction(column='category_name', correction_type='corrections')

        for original, correction in category_name_corrections.items():
            df_items = norm.replace_value_when(df_items, column='Category Name', condition=df_items['Category Name'] == original, replacement=correction)

        df_items = df_items.join(df_categories, on='Category Name', how='left')
        df_items = df_items.drop('Category Name')
    except Exception as e:
        print (f'Error raised at Apply Category Name Corrections section: {e}')


    # Keep Latest Row on [Item Number and Vendor Number]
    try:
        keep_latest_partition_cols = ['Item Number', 'Vendor Number', 'Category Number']
        df_items = norm.keep_latest_row(df_items, partition_cols=keep_latest_partition_cols, order_by_col='Date')
    except Exception as e:
        print (f'Error raised at Keep Latest Row section: {e}')


    # Finalizing
    try:
        column_order = ['Item Number', 'Item Description', 'Category Number', 'Vendor Number', 'Pack', 'Bottle Volume ml', 'State Bottle cost', 'State Bottle Retail']
        df_items = df_items.select(column_order)

        df_items = norm.add_prefix(df_items, column='Item Number', prefix='ITM_')
        df_items = df_items.orderBy(df_items['Item Number'])
    except Exception as e:
        print (f'Error raised at Finalizing section: {e}')


    # Export Processed Data in '.parquet'
    try:
        output_table = 'item'
        norm.write_data(df_to_export=df_items, output_path=conf.get_path(output_table))
    except Exception as e:
        print (f'Error raised at Export Processed Data section: {e}')

    # df_items.show()

except Exception as e:
    print (f'An unexpected error occurred: {e}')