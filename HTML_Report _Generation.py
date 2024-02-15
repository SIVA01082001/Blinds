import os
import pandas as pd
from datetime import datetime
from google.cloud import bigquery

def analyze_dataframe(df, dataset_name):
    # Get the number of rows and columns
    num_rows, num_cols = df.shape

    df_str = df.map(lambda x: str(x) if instance (x,np.ndarray) else x)
    columns_table = pd.DataFrame({
        'Dataset': dataset_id,
        'Table_Name' : table_id,
        'Column_Name' : df.columns,
        'Data Type': df_str.dtypes,
        'Null Values': df_str.isnull().sum(),
        'Null Percentage': ((df_str.isnull().sum() / len(df_str)) * 100).map("{:.2f}%".format),
        'Distinct Values': df_str.nunique(),
        'Distinct Percentage': ((df_str.nunique() / len(df_str)) * 100).map("{:.2f}%".format),
        'Duplicate Values': df_str.apply(lambda x: len(x) - len(x.unique())),
        'Duplicate Percentage': ((df_str.apply(lambda x: len(x) - len(x.unique()))) / len(df) * 100).map("{:.2f}%".format)})

    numeric_columns = df.select_dtypes(include=['number']).columns

    # Count zeros and calculate the percentage of zeros for each row
    zero_count = df[numeric_columns].eq(0).sum(axis=1)
    zero_percentage = (zero_count / len(numeric_columns) * 100).map("{:.2f}%".format)
    columns_table["Zero's Count"] = zero_count
    columns_table["Zero's Percentage"] = zero_percentage

   # Check if there are numeric columns before calculating statistics
    if not numeric_columns.empty:
        # Calculate average, minimum, maximum, median, and mode for numeric columns and add new columns
        columns_table['Minimum_value'] = df[numeric_columns].min().map("{:.2f}".format)
        columns_table['Maximum_value'] = df[numeric_columns].max().map("{:.2f}".format)
        columns_table['Mid_value'] = df[numeric_columns].median().map("{:.2f}".format)
        columns_table['Most_common_value'] = df[numeric_columns].mode().iloc[0].map("{:.2f}".format)
        columns_table['Average_value'] = df[numeric_columns].mean().map("{:.2f}".format)
    else:
        # If there are no numeric columns, set these columns to a placeholder value or handle as needed
        columns_table['Minimum_value'] = None
        columns_table['Maximum_value'] = None
        columns_table['Mid_value'] = None
        columns_table['Most_common_value'] = None
        columns_table['Average_value'] = None

    

    # Apply conditional formatting to highlight values in 'Null Percentage' column
    def highlight_percentage(val):
        return 'background-color: red' if float(val.rstrip('%')) > 1 else ''

    # Apply conditional formatting to highlight values in 'Distinct Values' column based on percentage
    def highlight_distinct(val):
        return 'background-color: green' if float(val.rstrip('%')) > 90 else ''

    # Apply conditional formatting to highlight values in 'Duplicate Values' column based on percentage
    def highlight_duplicates(val):
        return 'background-color: yellow' if float(val.rstrip('%')) > 95 else ('background-color: green' if float(val.rstrip('%')) < 5 else '')

    # Apply conditional formatting to highlight values in 'Average', 'Minimum', 'Maximum', 'Mid', 'Most_common', and 'Mode' columns
    def highlight_numeric(val):
        return ''

    styled_columns_table = (
        columns_table.style
        .map(highlight_percentage, subset=['Null Percentage'])
        .map(highlight_distinct, subset=['Distinct Percentage'])
        .map(highlight_duplicates, subset=['Duplicate Percentage'])
        .map(highlight_numeric, subset=['Minimum_value', 'Maximum_value', 'Mid_value', 'Most_common_value', 'Average_value'])
        .map(highlight_numeric, subset=["Zero's Count", "Zero's Percentage"])  # Added highlight for numeric columns
        .set_table_styles([{
            'selector': 'thead tr',
            'props': 'border-bottom: 1px solid black;'
        }, {
            'selector': 'tbody tr',
            'props': 'border-top: 1px solid black; border-bottom: 1px solid black;'
        }, {
            'selector': 'th',
            'props': 'border-right: 1px solid black;'
        }, {
            'selector': 'td',
            'props': 'border-right: 1px solid black;'
        }])
    )

    return styled_columns_table, f"<p style='font-weight: bold; font-size: 25px; text-decoration: underline;'>{dataset_name}</p><p>Number of Rows: {num_rows}</p><p>Number of Columns: {num_cols}</p>"

def analyze_bigquery_table(project_id, dataset_id, table_id):
    # Initialize BigQuery client
    client = bigquery.Client(project=project_id)

    # Construct the fully qualified table reference
    table_ref = f"{project_id}.{dataset_id}.{table_id}"

    # Fetch the table schema
    table = client.get_table(table_ref)
    columns = [field.name for field in table.schema]

    # Read the full table into a DataFrame
    query = f"SELECT * FROM `{table_ref}`"
    df = client.query(query).to_dataframe()

    # Call the existing analyze_dataframe function
    styled_output, dataset_info = analyze_dataframe(df, f"{project_id}.{dataset_id}.{table_id}")

    return styled_output, dataset_info
#---------------------------------------------------------------------------------------------------------------------------
# List of GCP BigQuery table details
bigquery_datasets = [
    ("your_project_id", "your_dataset_id", "your_table_id1"),
    ("your_project_id", "your_dataset_id", "your_table_id2"),
    # Add more GCP table details as needed
]

# Add timestamp for versioning
timestamp = datetime.now().strftime("%Y%m%d%H%M")

# Initialize a string to store the combined HTML output
combined_html_output = ""

##Creating DF to store output
combined_dataframe=pd.DataFrame()

# Add JavaScript code for filtering by dataset
javascript_code = """
<script>
function filterByDataset() {
    var selectedDataset = document.getElementById("datasetDropdown").value;
    var tables = document.getElementsByClassName("dataset-table");
    for (var i = 0; i < tables.length; i++) {
        var table = tables[i];
        if (!selectedDataset || table.dataset.datasetName === selectedDataset) {
            table.style.display = 'block';
        } else {
            table.style.display = 'none';
        }
    }
}
</script>
"""

# Add filter dropdown with increased box size
# Modify the filter_dropdown string to make "Select Dataset" bold
filter_dropdown = "<div style='text-align: left; font-size: 18px; font-weight: bold;'>Select Dataset:</div>"
filter_dropdown += "<select id='datasetDropdown' style='height: 35px; width: 300px;' onchange='filterByDataset()'><option value=''>All Datasets</option>"
for project_id, dataset_id, table_id in bigquery_datasets:
    filter_dropdown += f"<option value='{project_id}.{dataset_id}.{table_id}'>{project_id}.{dataset_id}.{table_id}</option>"
filter_dropdown += "</select>"

# Add a darker and bold line after the filter dropdown
filter_dropdown += "<hr style='border-style: solid; border-width: 3px; border-color: #000;'>"

# Iterate through the list of GCP BigQuery table details
for project_id, dataset_id, table_id in bigquery_datasets:
    # Analyze the BigQuery table using the modified function
    styled_output, dataset_info = analyze_bigquery_table(project_id, dataset_id, table_id)

    # Append the HTML output to the combined output string
    combined_html_output += f"<div class='dataset-table' data-dataset-name='{project_id}.{dataset_id}.{table_id}'>"
    combined_html_output += dataset_info
    combined_html_output += styled_output.to_html(escape=False)
    combined_html_output += "</div>"

    # Add a separator line
    combined_html_output += "<hr>"

    combined_dataframe=pd.concat([combined_dataframe,styled_output.data],ignore_index=True)
    
# Save the combined HTML output to a single file with JavaScript and filter dropdown
html_output_path = f'DATA_SUMMARY_{timestamp}.html'
with open(html_output_path, 'w') as file:
    file.write(javascript_code)
    file.write(filter_dropdown)
    file.write(combined_html_output)

print(f"Combined HTML output with filter saved to {html_output_path}")


excel_output_path = f"DATA_SUMMARY_{timestamp}.xlsx'
combined_dataframe.to_excel(excel_output_path,index=False)
print(f"Combined Excel output saved to {excel_output_path}")
