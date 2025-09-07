# DataFrame Comparison Tool

This project provides a reusable tool for comparing two datasets (e.g., Spark DataFrames, tables).  
It is designed to support data migration and validation efforts, such as ensuring data consistency between an on-premises warehouse and a new cloud data platform.  

The tool uses **PySpark** for distributed data processing and **Streamlit** for interactive visualization of results.  

---

## Features

- **Schema comparison**  
  Detects column name and data type mismatches between two dataframes.  

- **Row count comparison**  
  Reports discrepancies in the number of rows between datasets.  

- **Row-level comparison**  
  Uses hashing to identify:  
  - Rows with identical data across dataframes  
  - Rows with the same identifier but different values  
  - Rows present only in one dataframe  

- **Summary statistics**  
  Provides counts and percentages of each row-level comparison category, viewable directly in Streamlit.  

---

## Assumptions

1. **Unique Identifier**  
   Each dataset contains a unique identifier column (or set of columns) that can be used for joining rows.  

2. **No Duplicate Column Names**  
   Column names within a dataframe are unique.  
   - If duplicate column names (e.g., two `salary` columns) were to appear, they would need to be resolved (e.g., by suffixing with `_1`, `_2`).  

3. **Shared Columns Only**  
   Comparisons are limited to columns that exist in both dataframes.  
   - Columns present in only one dataframe are excluded from value comparisons.  

4. **Schema Type Differences Ignored**  
   If a column exists in both dataframes but has different data types, it is excluded from comparisons.  
   - Resolving type mismatches would require additional cleansing and transformation logic.  

---

## Running Locally

You can run the tool either in a **local virtual environment** or in a **Docker container**.  

### Option 1: Local Virtual Environment
Requirements:
- Python 3.10
- PySpark and required Spark components installed in your system
- Streamlit, loguru, PySpark

Setup:
```bash
python3.10 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
streamlit run app.py
