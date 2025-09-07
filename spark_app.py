from typing import List, Literal, Dict, Set, Union
from pyspark.sql import SparkSession
from pyspark.sql import DataFrame
import pyspark.sql.functions as func
from loguru import logger


def get_spark_session() -> SparkSession:
    """
    Create and return a Spark session for the comparison tool.

    Returns:
        SparkSession: An active Spark session with WARN log level.
    """
    spark = SparkSession.builder.appName("Compare_dfs").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark

def get_column_list(dataframe: DataFrame) -> List[str]:
    """
    Retrieve column names from a dataframe.

    Args:
        dataframe (DataFrame): Input PySpark dataframe.

    Returns:
        List[str]: List of column names.
    """
    return dataframe.columns

def check_are_columns_list_the_same(first_columns_list: List[str], second_columns_list:List[str]) -> tuple[Literal[False], set[str]] | tuple[Literal[True], set[str]]:
    """
    Compare two lists of columns and return whether they are identical.

    Args:
        first_columns_list (List[str]): Column names of the first dataframe.
        second_columns_list (List[str]): Column names of the second dataframe.

    Returns:
        tuple:
            - bool: True if column sets are identical, False otherwise.
            - set[str]: Shared columns between both dataframes.
    """
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
    """
    Count the number of rows in a dataframe.

    Args:
        dataframe (DataFrame): Input PySpark dataframe.

    Returns:
        int: Number of rows in the dataframe.
    """
    return dataframe.count()

def compare_records_count(first_df_count: int, second_df_count:int) -> bool:
    """
    Compare row counts between two dataframes.

    Args:
        first_df_count (int): Row count of the first dataframe.
        second_df_count (int): Row count of the second dataframe.

    Returns:
        bool: True if row counts are equal, False otherwise.
    """
    if first_df_count != second_df_count:
        logger.info(f"Discrepancy in row count. First df row count: {first_df_count}. Second df row count: {second_df_count}")
        return False
    else:
        logger.info(f"Dataframes have the same row count: {first_df_count}")
        return True

def get_schema_details(dataframe: DataFrame) -> dict[str, str]:
    """
    Extract schema details (column name → data type mapping).

    Args:
        dataframe (DataFrame): Input PySpark dataframe.

    Returns:
        dict[str, str]: Mapping of column names to their data type.
    """
    schema_dict = {}
    for field in dataframe.schema:
        schema_dict[field.name]=field.dataType.typeName()
    return schema_dict

def compare_schemas(first_df_schema: Dict[str,str], second_df_schema: Dict[str,str], columns_list: Set[str]) -> tuple[bool, list[str], list[str]]:
    """
    Compare schemas of two dataframes on shared columns.

    Args:
        first_df_schema (dict[str, str]): Schema of the first dataframe.
        second_df_schema (dict[str, str]): Schema of the second dataframe.
        columns_list (Set[str]): Shared columns between dataframes.

    Returns:
        tuple:
            - bool: True if schemas are identical, False otherwise.
            - list[str]: List of valid (matching) schema fields.
            - list[str]: List of invalid (mismatching) schema fields.
    """
    valid_schema_fields = []
    invalid_schemas_fields = []
    schemas_are_equal = True
    for field in columns_list:
        if not first_df_schema[field] == second_df_schema[field]:
            logger.info(f"Schema mismatch for field: {field}. First dataframe type: {first_df_schema[field]}, second dataframe type: {second_df_schema[field]}")
            schemas_are_equal = False
            invalid_schemas_fields.append(field)
        else:
            valid_schema_fields.append(field)
    return schemas_are_equal, valid_schema_fields, invalid_schemas_fields


def add_hash_column(dataframe: DataFrame, hash_suffix: str, exclude: Union[None, List[str]] = None) -> DataFrame:
    """
    Add a hash column to a dataframe based on its content.

    Args:
        dataframe (DataFrame): Input PySpark dataframe.
        hash_suffix (str): Suffix for the hash column name.
        exclude (list[str], optional): Columns to exclude from the hash calculation.

    Returns:
        DataFrame: Dataframe with an additional hash column.
    """
    columns_to_hash = dataframe.columns
    if exclude:
        columns_to_hash = [col for col in columns_to_hash if col not in exclude]
    return dataframe.withColumn(f"hash{hash_suffix}", func.hash(*columns_to_hash))


def get_summary_of_dataframes(first_dataframe: DataFrame, second_dataframe: DataFrame, unique_identifier_columns_names: List[str]) -> DataFrame:
    """
    Generate a summary of row-level comparison between two dataframes.

    Args:
        first_dataframe (DataFrame): First dataframe with hash column.
        second_dataframe (DataFrame): Second dataframe with hash column.
        unique_identifier_columns_names (List[str]): Identifier columns used for joins.

    Returns:
        DataFrame: Summary dataframe containing counts and percentages for:
            - Matching rows
            - Non-matching rows
            - Rows present only in one dataframe
    """    
    identifiers_with_hash_1 = unique_identifier_columns_names.copy()
    identifiers_with_hash_2 = unique_identifier_columns_names.copy()
    identifiers_with_hash_1.append('hash_1')
    identifiers_with_hash_2.append('hash_2')
    summary_df = first_dataframe.select(identifiers_with_hash_1).join(second_dataframe.select(identifiers_with_hash_2),on=unique_identifier_columns_names, how='full_outer').withColumn("status", 
        func.when(condition=func.col('hash_1') == func.col('hash_2'), value='row_data_matching_between_dfs')
        .when(condition=func.col(col='hash_1') != func.col('hash_2'), value = 'row_data_not_matching_between_dfs')
        .when(condition=func.col(col='hash_1').isNull() & func.col('hash_2').isNotNull(), value='row_present_only_in_df2')
        .when(condition=func.col(col='hash_2').isNull() & func.col('hash_1').isNotNull(), value='row_present_only_in_df1')
        )
    total_count = summary_df.count()
    stats_df =  summary_df\
                .groupBy("status")\
                .agg(func.count("*").alias("row_count"))\
                .withColumn("percentage_of_all_ids", func.round((func.col("row_count") / func.lit(total_count) * 100).cast("double"),2))
    return stats_df


def convert_json_to_pyspark_df(json_oobject: List[dict], spark_object: SparkSession) -> DataFrame:
    """
    Convert a JSON-like Python object (list of dicts) into a PySpark dataframe.

    Args:
        json_object (List[dict]): Input list of dictionaries.
        spark_object (SparkSession): Active Spark session.

    Returns:
        DataFrame: PySpark dataframe created from the JSON object.
    """
    return spark_object.createDataFrame(data=json_oobject)


def compare_dataframes(dataframe1: DataFrame, dataframe2: DataFrame, unique_identifier_columns_names: list[str]) -> DataFrame:
    """
    Compare two dataframes and generate a summary of similarities/differences.

    Steps:
        1. Check and align columns between dataframes.
        2. Compare row counts.
        3. Compare schemas and drop mismatched columns.
        4. Add hash columns to detect row-level differences.
        5. Generate summary statistics of row matches/mismatches.

    Args:
        dataframe1 (DataFrame): First PySpark dataframe.
        dataframe2 (DataFrame): Second PySpark dataframe.
        unique_identifier_columns_names (list[str]): Identifier columns used for joins.

    Returns:
        DataFrame: Summary dataframe with row counts and percentages per status.
    """
    first_df_columns_list = get_column_list(dataframe=dataframe1)
    second_df_columns_list = get_column_list(dataframe=dataframe2)
    are_columns_lists_the_same, shared_columns = check_are_columns_list_the_same(first_df_columns_list,second_df_columns_list)
    if not are_columns_lists_the_same:
        dataframe1 = dataframe1.select(list(shared_columns))
        dataframe2 = dataframe2.select(list(shared_columns))
    first_df_record_count = get_records_count(dataframe=dataframe1)
    second_df_record_count = get_records_count(dataframe=dataframe2)
    compare_records_count(first_df_count=first_df_record_count,second_df_count=second_df_record_count)
    first_df_schema = get_schema_details(dataframe=dataframe1)
    second_df_schema = get_schema_details(dataframe=dataframe2)
    schemas_are_equal, valid_schema_fields, invalid_schemas_fields = compare_schemas(first_df_schema=first_df_schema, second_df_schema=second_df_schema, columns_list=shared_columns)
    if not schemas_are_equal:
        dataframe1 = dataframe1.select(list(valid_schema_fields))
        dataframe2 = dataframe2.select(list(valid_schema_fields))        
    dataframe1 = add_hash_column(dataframe=dataframe1,hash_suffix="_1", exclude=unique_identifier_columns_names)
    dataframe2 = add_hash_column(dataframe=dataframe2,hash_suffix="_2", exclude=unique_identifier_columns_names)
    stats_df = get_summary_of_dataframes(first_dataframe=dataframe1, second_dataframe=dataframe2, unique_identifier_columns_names=unique_identifier_columns_names)
    return stats_df