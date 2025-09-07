import json
import streamlit as st
import pandas as pd
import spark_app

sample_data1 = [
    {"id": 1, "name": "Alice",   "age": 29, "department": "Engineering", "salary": 85000.0},
    {"id": 2, "name": "Bob",     "age": 35, "department": "HR",          "salary": 60000.0},
    {"id": 3, "name": "Charlie", "age": 40, "department": "Engineering", "salary": 95000.0},
    {"id": 4, "name": "Diana",   "age": 30, "department": "Marketing",   "salary": 72000.0},
    {"id": 5, "name": "Ethan",   "age": 26, "department": "Finance",     "salary": 65000.0},
    {"id": 6, "name": "Fiona",   "age": 28, "department": "Finance",     "salary": 67000.0},
    {"id": 7, "name": "George",  "age": 31, "department": "Engineering", "salary": 78000.0}
]

sample_data2 = [
    {"id": 1, "name": "Alice",   "age": 29, "department": "Engineering",     "salary": "$87,000", "hire_date": "2020-01-15"},
    {"id": 2, "name": "Bob",     "age": 36, "department": "Human Resources", "salary": "61000",   "hire_date": "2018-03-20"},
    {"id": 3, "name": "Charlie", "age": 40, "department": "Engineering",     "salary": None,      "hire_date": "2015-07-01"},
    {"id": 4, "name": "Diana",   "age": 30, "department": "Marketing",       "salary": "72k",     "hire_date": "2019-09-10"},
    {"id": 8, "name": "Helen",   "age": 27, "department": "Design",          "salary": "69000",   "hire_date": "2021-11-05"},
    {"id": 9, "name": "Ian",     "age": 34, "department": "Sales",           "salary": "71,000",  "hire_date": "2022-02-01"},
]

spark = spark_app.get_spark_session()

mode = st.selectbox("Select input mode", ["Manual JSON input", "File upload"])

df1_json, df2_json = None, None
unique_identifier_columns_names = []

if mode == "Manual JSON input":

    with st.form("manual_form"):
        df_1_text = st.text_area("First DataFrame JSON", value=json.dumps(sample_data1, indent=2))
        df_2_text = st.text_area("Second DataFrame JSON", value=json.dumps(sample_data2, indent=2))
        raw_input = st.text_input("Unique identifier columns (comma-separated)", value="id")
        submit_button = st.form_submit_button("Compare DataFrames")

    if submit_button:
        try:
            df1_json = json.loads(df_1_text)
            df2_json = json.loads(df_2_text)
            unique_identifier_columns_names = [col.strip() for col in raw_input.split(",") if col.strip()]
        except json.JSONDecodeError as e:
            st.error(f"Invalid JSON format: {e}")

elif mode == "File upload":

    with st.form("file_form"):
        file_1 = st.file_uploader("Upload first file", type=["csv", "json"])
        file_2 = st.file_uploader("Upload second file", type=["csv", "json"])
        raw_input = st.text_input("Unique identifier columns (comma-separated)", value="id")
        submit_button = st.form_submit_button("Compare DataFrames")

    if submit_button:
        try:
            unique_identifier_columns_names = [col.strip() for col in raw_input.split(",") if col.strip()]

            def file_to_json(file):
                if file.name.endswith(".csv"):
                    df = pd.read_csv(file)
                    return df.to_dict(orient="records")
                elif file.name.endswith(".json"):
                    return json.load(file)
                else:
                    return []

            if file_1 and file_2:
                df1_json = file_to_json(file_1)
                df2_json = file_to_json(file_2)
            else:
                st.warning("Please upload both files")

        except Exception as e:
            st.error(f"Error reading files: {e}")


if df1_json and df2_json:
    try:
        dataframe1 = spark_app.convert_json_to_pyspark_df(df1_json, spark_object=spark)
        dataframe2 = spark_app.convert_json_to_pyspark_df(df2_json, spark_object=spark)
        first_df_columns_list = spark_app.get_column_list(dataframe1)
        second_df_columns_list = spark_app.get_column_list(dataframe2)
        are_columns_lists_the_same, shared_columns = spark_app.check_are_columns_list_the_same(first_df_columns_list,second_df_columns_list)
        if not are_columns_lists_the_same:
            st.warning("Discrepancy in columns detected. Droping columns which are not shared between dataframes")
            st.warning(f"Dataframe1 columns: {','.join(first_df_columns_list)}")
            st.warning(f"Dataframe2 columns: {','.join(second_df_columns_list)}")
            st.warning(f"Shared columns: {','.join(shared_columns)}")
            dataframe1 = dataframe1.select(list(shared_columns))
            dataframe2 = dataframe2.select(list(shared_columns))
        else:
            st.write("Columns are matching between dataframes")
        first_df_record_count = spark_app.get_records_count(dataframe1)
        second_df_record_count = spark_app.get_records_count(dataframe2)
        is_recod_count_matching = spark_app.compare_records_count(first_df_count=first_df_record_count,second_df_count=second_df_record_count)
        if not is_recod_count_matching:
            st.warning(f"Discrepancy in row count. First df row count: {first_df_record_count}. Second df row count: {second_df_record_count}")
        else:
            st.write(f"Dataframes have the same row count: {first_df_record_count}")
        first_df_schema = spark_app.get_schema_details(dataframe1)
        second_df_schema = spark_app.get_schema_details(dataframe2)
        schemas_are_equal, valid_schema_fields, invalid_schemas_fields = spark_app.compare_schemas(first_df_schema=first_df_schema, second_df_schema=second_df_schema, columns_list=shared_columns)
        if not schemas_are_equal:
            dataframe1 = dataframe1.select(list(valid_schema_fields))
            dataframe2 = dataframe2.select(list(valid_schema_fields))
            st.warning("Schema mismatch detected. Dropping fields which are affected")
            for field in invalid_schemas_fields:
                st.warning(f"Schema mismatch for field: {field}. First dataframe type: {first_df_schema[field]}, second dataframe type: {second_df_schema[field]}")
        dataframe1 = spark_app.add_hash_column(dataframe1,hash_suffix="_1", exclude=unique_identifier_columns_names)
        dataframe2 = spark_app.add_hash_column(dataframe2,hash_suffix="_2", exclude=unique_identifier_columns_names)
        stats_df = spark_app.get_summary_of_dataframes(first_dataframe=dataframe1, second_dataframe=dataframe2, unique_identifier_columns_names=unique_identifier_columns_names)
        st.dataframe(stats_df.toPandas())
        st.success("Comparison completed!")
    except Exception as e:
        st.error(f"Error processing DataFrames: {e}")