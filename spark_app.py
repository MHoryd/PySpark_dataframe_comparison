from typing import List, Literal, Dict, Set, Union
from pyspark.sql import SparkSession
from pyspark.sql import DataFrame
import pyspark.sql.functions as func
from loguru import logger


def get_spark_session() -> SparkSession:
    spark = SparkSession.builder.appName("Compare_dfs").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark

def get_column_list(dataframe: DataFrame) -> List[str]:
    return dataframe.columns

def check_are_columns_list_the_same(first_columns_list: List[str], second_columns_list:List[str]) -> tuple[Literal[False], set[str]] | tuple[Literal[True], set[str]]:
    shared_columns = set(first_columns_list).intersection(second_columns_list)
    if not set(first_columns_list) == set(second_columns_list):
        logger.info("Discrepancy in columns detected")
        logger.info(f"Dataframe1 columns: {','.join(first_columns_list)}")
        logger.info(f"Dataframe2 columns: {','.join(second_columns_list)}")
        logger.info(f"Shared columns: {','.join(shared_columns)}")
        return False, shared_columns
    else:
        logger.info("Both dataframes have the same columns")
        return True, shared_columns

def get_records_count(dataframe: DataFrame) -> int:
    return dataframe.count()

def compare_records_count(first_df_count: int, second_df_count:int):
    if first_df_count != second_df_count:
        logger.info(f"Discrepancy in row count. First df row count: {first_df_count}. Second df row count: {second_df_count}")
    else:
        logger.info(f"Dataframes have the same row count: {first_df_count}")

def get_schema_details(dataframe: DataFrame):
    schema_dict = {}
    for field in dataframe.schema:
        schema_dict[field.name]=field.dataType.typeName()
    return schema_dict

def compare_schemas(first_df_schema: Dict[str,str], second_df_schema: Dict[str,str], columns_list: Set[str])-> tuple[bool, list[str]]:
    valid_schema_fields = []
    schemas_are_equal = True
    for field in columns_list:
        if not first_df_schema[field] == second_df_schema[field]:
            logger.info(f"Schema mismatch for field: {field}. First dataframe type: {first_df_schema[field]}, second dataframe type: {second_df_schema[field]}")
            schemas_are_equal = False
        else:
            valid_schema_fields.append(field)
    return schemas_are_equal, valid_schema_fields


def add_hash_column(dataframe: DataFrame, exclude: Union[None, List[str]] = None):
    columns_to_hash = dataframe.columns
    if exclude:
        columns_to_hash = [col for col in columns_to_hash if col not in exclude]
    return dataframe.withColumn('hash', func.hash(*columns_to_hash))


def compare_dataframes(dataframe1: DataFrame, dataframe2: DataFrame, unique_identifier_columns_names: list[str]) -> None:
    first_df_columns_list = get_column_list(dataframe1)
    second_df_columns_list = get_column_list(dataframe2)
    are_columns_lists_the_same, shared_columns = check_are_columns_list_the_same(first_df_columns_list,second_df_columns_list)
    if not are_columns_lists_the_same:
        dataframe1 = dataframe1.select(list(shared_columns))
        dataframe2 = dataframe2.select(list(shared_columns))
    first_df_record_count = get_records_count(dataframe1)
    second_df_record_count = get_records_count(dataframe2)
    compare_records_count(first_df_count=first_df_record_count,second_df_count=second_df_record_count)
    first_df_schema = get_schema_details(dataframe1)
    second_df_schema = get_schema_details(dataframe2)
    schemas_are_equal, valid_schema_fields = compare_schemas(first_df_schema=first_df_schema, second_df_schema=second_df_schema, columns_list=shared_columns)
    if not schemas_are_equal:
        dataframe1 = dataframe1.select(list(valid_schema_fields))
        dataframe2 = dataframe2.select(list(valid_schema_fields))        
    dataframe1 = add_hash_column(dataframe1, exclude=unique_identifier_columns_names)
    dataframe2 = add_hash_column(dataframe2,  exclude=unique_identifier_columns_names)
    unique_identifier_columns_names.append('hash')
    rows_with_same_values = dataframe1.select(unique_identifier_columns_names).join(dataframe2.select(unique_identifier_columns_names),on=unique_identifier_columns_names, how='inner').count()
    logger.info(f"""There are {rows_with_same_values} rows with the same data in joined dataframes which is {round(rows_with_same_values / first_df_record_count * 100,2)}% of first dataframe total
                and {round(rows_with_same_values / second_df_record_count * 100,2)}% of second dataframe total""")