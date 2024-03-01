from google.cloud import bigquery
import pandas as pd
from collections import Counter

# Initialize BigQuery client
client = bigquery.Client(project="analytics-ci-thd")

# Define your table and dataset
table_id = "analytics-views-thd.CONTACT_CENTERS.AVAYA_HAGENT"

dataset_name ="CONTACT_CENTERS"

table_name = "AVAYA_HAGENT"

# Get table schema to determine column names and types
table = client.get_table(table_id)
columns = [field.name for field in table.schema]
column_types = {field.name: field.field_type for field in table.schema}

# Get total number of rows in the table
query = f"""
SELECT COUNT(*) AS total_rows
FROM `{table_id}`
"""
query_job = client.query(query)
result = query_job.result()
total_rows = list(result)[0]["total_rows"]

---------------------------------------------------------------------------

# Initialize list to store results
null_results = []

# Iterate over each column
for column in columns:
    # Null values
    query = f"""
    SELECT COUNT(*) AS null_count
    FROM `{table_id}`
    WHERE {column} IS NULL
    """
    query_job = client.query(query)
    result = query_job.result()
    null_count = list(result)[0]["null_count"]
    null_percentage = (null_count / total_rows) * 100
    
    null_results.append({
        "Dataset":dataset_name, 
        "Table_Name": table_name,
        "Column_Name": column,
        "Column_Type": column_types[column],
        "Null Values": null_count,
        "Null Percentage": f"{null_percentage:.2f}%"
    })
    

# Convert results list to DataFrame
null_df = pd.DataFrame(null_results)

# Convert Null_Values column to integer and Null_Percentage column to string
null_df['Null Values'] = null_df['Null Values'].astype(pd.Int64Dtype())

null_df

---------------------------------------------------------------

# Initialize list to store results
distinct_results = []

# Iterate over each column
for column in columns:
    # Distinct values
    query = f"""
    SELECT COUNT(DISTINCT {column}) AS distinct_count
    FROM `{table_id}`
    """
    query_job = client.query(query)
    result = query_job.result()
    distinct_count = list(result)[0]["distinct_count"]
    distinct_percentage = (distinct_count / total_rows) * 100
    distinct_results.append({
        "Dataset":dataset_name, 
        "Table_Name": table_name,
        "Column_Name": column,
        "Column_Type": column_types[column],
        "Distinct Values": distinct_count,
        "Distinct Percentage": f"{distinct_percentage:.2f}%"
    })
    
# Convert results list to DataFrame
distinct_df = pd.DataFrame(distinct_results)

# Convert Null_Values column to integer and Null_Percentage column to string
distinct_df['Distinct Values'] = distinct_df['Distinct Values'].astype(pd.Int64Dtype())

distinct_df

--------------------------------------------------------------------------

import pandas as pd

# Initialize list to store results
duplicate_results = []

# Iterate over each column
for column in columns:
    # Initialize dictionary to store result for the current column
    duplicate_result_dict = {"Dataset":dataset_name, "Table_Name": table_name, "Column_Name": column, "Column_Type_Type": column_types[column]}

    # Duplicate values and duplicate percentage
    query = f"""
    SELECT COUNT(DISTINCT {column}) AS distinct_count,
           COUNT(*) AS total_count
    FROM `{table_id}`
    """
    query_job = client.query(query)
    
    # Fetching results from the query job
    query_results = query_job.result()
    for row in query_results:
        total_count = row['total_count']
        distinct_count = row['distinct_count']
    
    duplicate_count = total_count - distinct_count
    duplicate_percentage = (duplicate_count / total_count) * 100

    # Update result dictionary
    duplicate_result_dict["Duplicate Values"] = duplicate_count
    duplicate_result_dict["Duplicate Percentage"] = f"{duplicate_percentage:.2f}%"

    # Append the result dictionary to the results list
    duplicate_results.append(duplicate_result_dict)

# Convert results list to DataFrame
duplicate_df = pd.DataFrame(duplicate_results)
duplicate_df['Duplicate Values'] = duplicate_df['Duplicate Values'].astype(pd.Int64Dtype())

duplicate_df

----------------------------------------------------------------------------------

zero_results = []

for column in columns:
    if column_types[column] == 'INTEGER' or column_types[column] == 'FLOAT':
        query = f"""
        SELECT COUNT({column}) AS zero_count
        FROM `{table_id}`
        WHERE {column} = 0
        """
        query_job = client.query(query)
        zero_count = list(query_job.result())[0]["zero_count"]
        zero_percentage = (zero_count / total_rows) * 100
        zero_results.append({
            "Dataset":dataset_name, 
            "Table_Name": table_name,
            "Column_Name": column,
            "Column_Type": column_types[column],
            "Zero's Count": zero_count,
            "Zero's Percentage": f"{zero_percentage:.2f}%"})            
    else:
        # Add None for non-numerical columns in stats_results
        zero_results.append({
            "Dataset":dataset_name, 
            "Table_Name": table_name,
            "Column_Name": column,
            "Column_Type": column_types[column],
            "Zero's Count": None,
            "Zero's Percentage": None
        })
        
zero_df = pd.DataFrame(zero_results)

# Convert Zero_Count column to integer 
zero_df["Zero's Count"] = zero_df["Zero's Count"].astype(pd.Int64Dtype())

zero_df

--------------------------------------------------------------

# Initialize list to store results
stats_results = []

# Iterate over each column
for column in columns:
    if column_types[column] == 'INTEGER' or column_types[column] == 'FLOAT':
        # Minimum value
        query = f"""
        SELECT MIN({column}) AS min_value
        FROM `{table_id}`
        """
        query_job = client.query(query)
        min_value = list(query_job.result())[0]["min_value"]

        # Maximum value
        query = f"""
        SELECT MAX({column}) AS max_value
        FROM `{table_id}`
        """
        query_job = client.query(query)
        max_value = list(query_job.result())[0]["max_value"]

        # Mid value (average of min and max)
        mid_value = (min_value + max_value) / 2

        # Most common value
        query = f"""
        SELECT {column} AS common_value, COUNT({column}) AS count
        FROM `{table_id}`
        GROUP BY {column}
        ORDER BY count DESC
        LIMIT 1
        """
        query_job = client.query(query)
        most_common_value = list(query_job.result())[0]["common_value"]

        # Average value
        query = f"""
        SELECT AVG({column}) AS avg_value
        FROM `{table_id}`
        """
        query_job = client.query(query)
        avg_value = list(query_job.result())[0]["avg_value"]

        stats_results.append({
            "Dataset":dataset_name,                 
            "Table_Name": table_name,
            "Column_Name": column,
            "Column_Type": column_types[column],
            "Minimum_value": min_value,
            "Maximum_value": max_value,
            "Mid_value": mid_value,
            "Most_common_value": most_common_value,
            "Average_value": avg_value
        })
    else:
        # Add None for non-numerical columns in stats_results
        stats_results.append({
            "Dataset":dataset_name, 
            "Table_Name": table_name,
            "Column_Name": column,
            "Column_Type": column_types[column],
            "Minimum_value": None,
            "Maximum_value": None,
            "Mid_value": None,
            "Most_common_value": None,
            "Average_value": None
        })

# Convert results list to DataFrame
stats_df = pd.DataFrame(stats_results)

stats_df

---------------------------------------------------------

# Merge all DataFrames on 'Table_Name' and 'Column_Name' columns
final_df = pd.merge(null_df, distinct_df, on=['Table_Name', 'Column_Name'], how='outer',suffixes=('','_new'))
final_df = pd.merge(final_df, duplicate_df, on=['Table_Name', 'Column_Name'], how='outer',suffixes=('','_new'))
final_df = pd.merge(final_df, zero_df, on=['Table_Name', 'Column_Name'], how='outer',suffixes=('','_new'))
final_df = pd.merge(final_df, stats_df, on=['Table_Name', 'Column_Name'], how='outer',suffixes=('','_new'))

final_df.drop(["Dataset_new","Column_Type_new","Column_Type_Type"],axis=1,inplace=True)

final_df

--------------------------------------------------------

excel_path = f'{table_name}_DATA_SUMMARY_tables.xlsx'
final_df.to_excel(excel_path,index=False)
print(f"{excel_path}_saved")
