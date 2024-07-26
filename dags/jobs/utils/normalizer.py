from typing import List, Any
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import initcap, upper, regexp_replace, when, row_number, concat, lit, trim, max, min, round, mode, to_date, coalesce, count, split
from pyspark.sql.window import Window
from pyspark.sql.types import IntegerType, StringType, DoubleType
from utils.configparser import ConfigParser


config_path = 'config.yaml'
conf = ConfigParser(config_path)


class Normalizer:

    '''
    A class for denormalizing data using Apache Spark.

    Args:
        spark (SparkSession): The Spark session object.

    Methods:
        read_data(file_path: str, format: str = 'csv', header: bool = True, inferSchema: bool = None, schema: Any = None) -> DataFrame:
            Reads data from a file into a Spark DataFrame.

        initcap(df_to_initcap: DataFrame, column: str) -> DataFrame:
            Converts the first letter of each word in the specified column to uppercase.

        upper(df_to_upper: DataFrame, column: str) -> DataFrame:
            Converts all characters in the specified column to uppercase.

        trim(df_to_trim: DataFrame, column: str) -> DataFrame:
            Trims leading and trailing spaces from the specified column.

        remove_double_spaces(df_to_remove_double_spaces: DataFrame, column: str) -> DataFrame:
            Removes double spaces from the specified column.

        remove_string(df_to_remove: DataFrame, column: str, to_remove: List[str]) -> DataFrame:
            Removes specified strings from the specified column.

        replace_string(df_to_replace: DataFrame, column: str, string: str, replacement: Any) -> DataFrame:
            Replaces a string with another string in the specified column.

        replace_value_when(df_to_replace: DataFrame, column: str, condition: Any, replacement: Any) -> DataFrame:
            Replaces values in a column based on 'when' condition.

        replace_value_coalesce(df_to_replace: DataFrame, column: str, replacement_column: str) -> DataFrame:
            Replaces values in a column with values from another column.

        generate_id(df_to_apply: DataFrame, window_columns: str, new_column_name: str) -> DataFrame:
            Generates unique ID for a DataFrame based on window columns.

        generate_max_id(df_to_apply: DataFrame, column: str) -> DataFrame:
            Generates unique IDs for rows with null values in the specified column, starting from the current maximum ID + 1.

        add_prefix(df_without_prefix: DataFrame, column: str, prefix: str) -> DataFrame:
            Adds a prefix to the values in the specified column.

        cast(df_to_cast: DataFrame, column: str, type: str, date_format: str = 'MM/dd/yyyy') -> DataFrame:
            Casts the specified column to a specified type.

        most_recent_True(df_to_identify: DataFrame, partition_cols: List[str], order_by_col: str, new_column_name: str) -> DataFrame:
            Marks the most recent row as True within partitions.

        keep_latest_row(df_to_keep_latest_row: DataFrame, partition_cols: List[str], order_by_col: str) -> DataFrame:
            Keeps only the latest row within partitions.

        decide_null_rows(df_to_decide: DataFrame, column_to_decide: str, partition_cols: List[str]) -> DataFrame:
            Decides rows to keep or drop based on null values within partitions.

        split_column(df_to_split: DataFrame, column_to_split: str, index: int, new_column_name: str, round_to: int = 6) -> DataFrame:
            Splits a column and rounds the values to a specified number of decimal places.

        mode_column(df_to_mode: DataFrame, column_to_mode: str, groupby_cols: List[str], alias: str) -> DataFrame:
            Computes the mode of a column within groups.

        mode_multiple_columns(df_to_mode: DataFrame, columns_to_mode: List[str], groupby_cols: List[str], aliases: List[str]) -> DataFrame:
            Computes the mode of multiple columns within groups.

        create_date_range(df_to_apply: DataFrame, groupby_cols: List[str], min_date_col: str, max_date_col: str) -> DataFrame:
            Creates date range columns from min and max date columns within groups.

        occurrence(df_to_identify: DataFrame, partition_cols: List[str], order_by_col: str, new_column_name: str) -> DataFrame:
            Adds occurrence numbers within partitions.

        write_data(df_to_export: DataFrame, output_path: str, format: str = 'parquet', mode: str = 'overwrite') -> None:
            Writes a DataFrame to a file in the specified format.
    '''
    
    def __init__(self, spark: SparkSession) -> None:
        self.spark = spark


    def read_data(self, file_path: str, format: str = 'csv', header: bool = True, inferSchema: bool = None, schema: Any = None) -> DataFrame:
        '''Reads data from a file into a Spark DataFrame.'''
        return self.spark.read.load(file_path, format=format, header=header, inferSchema=inferSchema, schema=schema)
    

    def load_data(self, data_files: List):
        data_frames = {}
        for file_name, data_format in data_files:
            try:
                file_path = conf.get_path(file_name)
                schema = conf.get_schema(file_name)
                df_file = self.read_data(file_path, format=data_format, schema=schema)
                data_frames[file_name] = df_file
            except Exception as e:
                print(f'Error raised when loading data from {file_name}: {e}')
        return data_frames

    
    def initcap(self, df_to_initcap: DataFrame, column: str) -> DataFrame:
        '''Converts the first letter of each word in the specified column to uppercase.'''
        return df_to_initcap.withColumn(column, initcap(df_to_initcap[column]))

    
    def upper(self, df_to_upper: DataFrame, column: str) -> DataFrame:
        '''Converts all characters in the specified column to uppercase.'''
        return df_to_upper.withColumn(column, upper(df_to_upper[column]))

    
    def trim(self, df_to_trim: DataFrame, column: str) -> DataFrame:
        '''Trims leading and trailing spaces from the specified column.'''
        return df_to_trim.withColumn(column, trim(df_to_trim[column]))

    
    def remove_double_spaces(self, df_to_remove_double_spaces: DataFrame, column: str) -> DataFrame:
        '''Removes double spaces from the specified column.'''
        return df_to_remove_double_spaces.withColumn(column, regexp_replace(df_to_remove_double_spaces[column], '  ', ' '))

    
    def remove_string(self, df_to_remove: DataFrame, column: str, to_remove: List[str]) -> DataFrame:
        '''Removes specified strings from the specified column.'''
        for item in to_remove:
            df_to_remove = df_to_remove.withColumn(column, regexp_replace(df_to_remove[column], item, ''))
        return df_to_remove

    
    def replace_string(self, df_to_replace: DataFrame, column: str, string: str, replacement: Any) -> DataFrame:
        '''Replaces a string with another string in the specified column.'''
        return df_to_replace.withColumn(column, regexp_replace(df_to_replace[column], string, replacement))


    def replace_value_when(self, df_to_replace: DataFrame, column: str, condition: Any, replacement: Any) -> DataFrame:
        '''Replaces values in a column based on 'when' condition.'''
        return df_to_replace.withColumn(column, when(condition, replacement).otherwise(df_to_replace[column]))

    
    def replace_value_coalesce(self, df_to_replace: DataFrame, column: str, replacement_column: str) -> DataFrame:
        '''Replaces values in a column with values from another column.'''
        return df_to_replace.withColumn(column, coalesce(df_to_replace[replacement_column], df_to_replace[column]))

    
    def generate_id(self, df_to_apply: DataFrame, window_columns: str, new_column_name: str) -> DataFrame:
        '''Generates unique ID for a DataFrame based on window columns.'''
        return df_to_apply.withColumn(new_column_name, row_number().over(Window.orderBy(window_columns)))

    
    def generate_max_id(self, df_to_apply: DataFrame, column: str) -> DataFrame:
        '''Generates unique IDs for rows with null values in the specified column, starting from the current maximum ID + 1.'''
        max_id = df_to_apply.select(max(column)).first()[0]
        df_to_apply = df_to_apply.withColumn('row_number', row_number().over(Window.orderBy(column)) + max_id)
        df_to_apply = df_to_apply.withColumn(column, when(df_to_apply[column].isNull(), df_to_apply['row_number']).otherwise(df_to_apply[column]))
        result_df = df_to_apply.drop('row_number')
        return result_df

    
    def add_prefix(self, df_without_prefix: DataFrame, column: str, prefix: str) -> DataFrame:
        '''Adds a prefix to the values in the specified column.'''
        return df_without_prefix.withColumn(column, concat(lit(prefix), df_without_prefix[column]))

    
    def cast(self, df_to_cast: DataFrame, column: str, type: str, date_format: str = 'MM/dd/yyyy') -> DataFrame:
        '''
        Casts the specified column to a specified type.
        type options: 'int', 'str', 'double', 'date'
        '''
        type_mapping = {
            'int': IntegerType(),
            'str': StringType(),
            'double': DoubleType()
        }
        if type in type_mapping:
            return df_to_cast.withColumn(column, df_to_cast[column].cast(type_mapping[type]))
        elif type == 'date':
            return df_to_cast.withColumn(column, to_date(df_to_cast[column], date_format))
        else:
            raise ValueError(f"Unsupported type '{type}'. Types can only be either of the following: 'int', 'str', 'double', 'date'")

    
    def most_recent_True(self, df_to_identify: DataFrame, partition_cols: List[str], order_by_col: str, new_column_name: str) -> DataFrame:
        '''Marks the most recent row as True within partitions.'''
        window_spec = Window.partitionBy(*partition_cols).orderBy(df_to_identify[order_by_col].desc())
        df_to_identify = df_to_identify.withColumn(new_column_name, row_number().over(window_spec) == 1)
        result_df = df_to_identify.withColumn(new_column_name, coalesce(df_to_identify[new_column_name], lit(False)))
        return result_df

    
    def keep_latest_row(self, df_to_keep_latest_row: DataFrame, partition_cols: List[str], order_by_col: str) -> DataFrame:
        '''Keeps only the latest row within partitions.'''
        window_spec = Window.partitionBy(*partition_cols).orderBy(df_to_keep_latest_row[order_by_col].desc())
        df_to_keep_latest_row = df_to_keep_latest_row.withColumn('row_number', row_number().over(window_spec))
        df_to_keep_latest_row = df_to_keep_latest_row.filter(df_to_keep_latest_row['row_number'] == 1)
        result_df = df_to_keep_latest_row.drop('row_number')
        return result_df

    
    def decide_null_rows(self, df_to_decide: DataFrame, column_to_decide: str, partition_cols: List[str]) -> DataFrame:
        '''Decides rows to keep or drop based on null values within partitions.'''
        window_spec = Window.partitionBy(*partition_cols)
        df_to_decide = df_to_decide.withColumn('non_null_count', count(when(df_to_decide[column_to_decide].isNotNull(), 1)).over(window_spec))
        df_to_decide = df_to_decide.filter((df_to_decide[column_to_decide].isNotNull()) | (df_to_decide['non_null_count'] == 0))
        result_df = df_to_decide.drop('non_null_count')
        return result_df

   
    def split_column(self, df_to_split: DataFrame, column_to_split: str, index: int, new_column_name: str, round_to: int = 6) -> DataFrame:
        '''Splits a column and rounds the values to a specified number of decimal places.'''
        return df_to_split.withColumn(new_column_name, round(regexp_replace(split(df_to_split[column_to_split], ' ')[index], '[()]', ''), round_to))

    
    def mode_column(self, df_to_mode: DataFrame, column_to_mode: str, groupby_cols: List[str], alias: str) -> DataFrame:
        '''Computes the mode of a column within groups.'''
        return df_to_mode.groupby(*groupby_cols).agg(mode(column_to_mode).alias(alias))
    
   
    def mode_multiple_columns(self, df_to_mode: DataFrame, columns_to_mode: List[str], groupby_cols: List[str], aliases: List[str]) -> DataFrame:
        '''Computes the mode of multiple columns within groups.'''
        mode_columns = []
        for col, alias in zip(columns_to_mode, aliases):
            mode_expr = mode(col).alias(alias)
            mode_columns.append(mode_expr)
        result_df = df_to_mode.groupby(*groupby_cols).agg(*mode_columns)
        return result_df
    
  
    def create_date_range(self, df_to_apply: DataFrame, groupby_cols: List[str], min_date_col: str, max_date_col: str) -> DataFrame:
        '''Creates date range columns from min and max date columns within groups.'''
        result_df = df_to_apply.groupby(*groupby_cols).agg(
            min(min_date_col).alias('From Date'),
            max(max_date_col).alias('To Date')
        )
        return result_df

  
    def occurrence(self, df_to_identify: DataFrame, partition_cols: List[str], order_by_col: str, new_column_name: str)-> DataFrame:
        '''Adds occurrence numbers within partitions.'''
        window_spec = Window.partitionBy(*partition_cols).orderBy(df_to_identify[order_by_col])
        result_df = df_to_identify.withColumn(new_column_name, row_number().over(window_spec))
        return result_df


    def write_data(self, df_to_export: DataFrame, output_path: str, format: str = 'parquet', mode: str = 'overwrite', num_partitions: int = None) -> None:
        '''Writes a DataFrame to a file in the specified format.'''
        if num_partitions:
            df_to_export = df_to_export.repartition(num_partitions)
        df_to_export.write.format(format).mode(mode).save(output_path)