from google.cloud import bigquery
import pandas as pd

def get_table_info(project_id, dataset_id):
    client = bigquery.Client(project='analytics-views-thd')

    # List all tables in the dataset
    dataset_ref = client.dataset(dataset_id)
    tables = list(client.list_tables(dataset_ref))

    table_info_list = []
    excluded_tables = []

    for table in tables:
      if table in table_id.starts_with('AVAYA') or table in table_id.starts_with('NEXIDIA') or table in table_id.starts_with('NICE') or table in table_id.starts_with('SALESFORCE'):
          try:
              client = bigquery.Client(project='analytics-views-thd')
              # Try to get table reference and fetch table schema
              table_ref = client.dataset(dataset_id).table(table.table_id)
              table_schema = client.get_table(table_ref).schema
  
              # Fetch a sample of the table to get the number of rows
              query = f"SELECT COUNT(*) FROM `{table_ref}`"
              client  = bigquery.Client(project='analytics-ci-thd')
              row_count = client.query(query).to_dataframe().iloc[0, 0]
  
              # Create a dictionary with table information
              table_info = {
                  "Table Name": table.table_id,
                  "Number of Rows": row_count,
                  "Number of Columns": len(table_schema)
              }
  
              table_info_list.append(table_info)
          except Exception as e:
              excluded_tables_info = {
                  "Table Name" : table.table_id
              }
              excluded_tables.append(excluded_tables_info)


    return pd.DataFrame(table_info_list), pd.DataFrame(excluded_lists)

# Replace with your GCP project_id and dataset_id
project_id = "analytics-views-thd"
dataset_id = "CONTACT_CENTERS"

# Get table information for the specified dataset
table_info_df, excluded_tables = get_table_info(project_id, dataset_id)

# Display the table information
print("Table Information:")
print(table_info_df)

# Display the excluded tables
print("\nExcluded Tables:")
print(excluded_tables)
