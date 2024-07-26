from pyspark.sql import SparkSession
from utils.normalizer import Normalizer
from utils.configparser import ConfigParser


spark = SparkSession.builder.appName('Category').master('local[*]').getOrCreate()
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

    columns = ['Category Name']
    df_categories = df_liquor_sales.select(columns)


    # Data Preprocessing
    try:
        df_categories = norm.initcap(df_categories, column='Category Name')
        df_categories = norm.remove_double_spaces(df_categories, column='Category Name')
        df_categories = df_categories.na.drop(subset='Category Name').dropDuplicates()
    except Exception as e:
        print (f'Error raised at Data Preprocessing section: {e}')


    # Apply Category Name Corrections
    try:
        category_name_corrections = conf.get_data_correction(column='category_name', correction_type='corrections')

        for original, correction in category_name_corrections.items():
            df_categories = norm.replace_value_when(df_categories, column='Category Name', condition=df_categories['Category Name'] == original, replacement=correction)

        df_categories = df_categories.dropDuplicates()
    except Exception as e:
        print (f'Error raised at Apply Category Name Corrections section: {e}')


    # Generate new ID for Category Number
    try:
        df_categories = norm.generate_id(df_categories, window_columns=['Category Name'], new_column_name='Category Number')
        df_categories = norm.add_prefix(df_categories, column='Category Number', prefix='CAT_')
    except Exception as e:
        print (f'Error raised at Generate new ID for Category Number section: {e}')


    # Finalizing
    try:
        column_order = ['Category Number', 'Category Name']
        df_categories = df_categories.select(column_order)
    except Exception as e:
        print (f'Error raised at Finalzing section: {e}')


    # Export Processed Data in '.parquet'
    try:
        output_table = 'category'
        norm.write_data(df_to_export=df_categories, output_path=conf.get_path(output_table))
    except Exception as e:
        print (f'Error raised at Export Processed Data section: {e}')

    # df_categories.show()

except Exception as e:
    print (f'An unexpected error occurred: {e}')

