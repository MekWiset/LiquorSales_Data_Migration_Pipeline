from pyspark.sql import SparkSession
from utils.normalizer import Normalizer
from utils.configparser import ConfigParser


spark = SparkSession.builder.appName('Store County').master('local[*]').getOrCreate()
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

    columns = ['County Number', 'County']
    df_counties = df_liquor_sales.select(columns)


    # Data Preprocessing
    try:
        df_counties = df_counties.withColumnRenamed('County', 'County Name')
        df_counties = norm.upper(df_counties, column='County Name')
        df_counties = df_counties.drop('County')
        df_counties = df_counties.na.drop(subset=['County Number'])
        df_counties = df_counties.dropDuplicates()
    except Exception as e:
        print (f'Error raised at Data Preprocessing section: {e}')


    # Apply County Name Corrections
    try:
        county_name_corrections = conf.get_data_correction(column='county_name', correction_type='corrections')

        for original, correction in county_name_corrections.items():
            df_counties = norm.replace_value_when(df_counties, column='County Name', condition=df_counties['County Name'] == original, replacement=correction)
            
        df_counties = df_counties.dropDuplicates()
    except Exception as e:
        print (f'Error raised at Apply County Name Corrections section: {e}')
        

    # Finalizing
    try:
        df_counties = norm.add_prefix(df_counties, column='County Number', prefix='CNT_')
        df_counties = df_counties.orderBy('County Name')
    except Exception as e:
        print (f'Error raised at Finalizing section: {e}')
        

    # Export Processed Data in '.parquet'
    try:
        output_table = conf.get_path('store_county')
        norm.write_data(df_to_export=df_counties, output_path=output_table)
    except Exception as e:
        print (f'Error raised at Export Processed Data section: {e}')

    # df_counties.show()

except Exception as e:
    print (f'An unexpected error occurred: {e}')

