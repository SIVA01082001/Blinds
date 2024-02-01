import os
import pandas as pd
from datetime import datetime
from google.cloud import bigquery

def analyze_dataframe(df, dataset_name):
    # Get the number of rows and columns
    num_rows, num_cols = df.shape

    # List of Columns with Data Types, Null Values, Null Percentage, Distinct Values, and Duplicate Values
    columns_table = pd.DataFrame({
        'Data Type': df.dtypes,
        'Null Values': df.isnull().sum(),
        'Null Percentage': ((df.isnull().sum() / len(df)) * 100).map("{:.2f}%".format),
        'Distinct Values': df.nunique(),
        'Distinct Percentage': ((df.nunique() / len(df)) * 100).map("{:.2f}%".format),
        'Duplicate Values': df.apply(lambda x: len(x) - len(x.unique())),
        'Duplicate Percentage': ((df.apply(lambda x: len(x) - len(x.unique()))) / len(df) * 100).map("{:.2f}%".format)})

    numeric_columns = df.select_dtypes(include=['number']).columns

    # Count zeros and calculate the percentage of zeros for each row
    zero_count = df[numeric_columns].eq(0).sum(axis=1)
    zero_percentage = (zero_count / len(numeric_columns) * 100).map("{:.2f}%".format)
    columns_table["Zero's Count"] = zero_count
    columns_table["Zero's Percentage"] = zero_percentage
   

    # Calculate average, minimum, maximum, median, and mode for numeric columns and add new columns
    columns_table['Minimum_value'] = df[numeric_columns].min().map("{:.2f}".format)
    columns_table['Maximum_value'] = df[numeric_columns].max().map("{:.2f}".format)
    columns_table['Mid_value'] = df[numeric_columns].median().map("{:.2f}".format)
    columns_table['Most_common_value'] = df[numeric_columns].mode().iloc[0].map("{:.2f}".format)
    columns_table['Average_value'] = df[numeric_columns].mean().map("{:.2f}".format)
    
     
    # Add a new column "Unique Values" based on the condition
    # columns_table['Unique Values'] = df.apply(lambda col: ', '.join(map(str, col.unique())) if col.nunique() / len(col) < 0.01 else None)

    # Convert 'Duplicate Percentage' values to numerical
    # columns_table['Duplicate Percentage'] = columns_table['Duplicate Percentage'].astype(str)

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
        .applymap(highlight_percentage, subset=['Null Percentage'])
        .applymap(highlight_distinct, subset=['Distinct Percentage'])
        .applymap(highlight_duplicates, subset=['Duplicate Percentage'])
        .applymap(highlight_numeric, subset=['Minimum_value', 'Maximum_value', 'Mid_value', 'Most_common_value', 'Average_value'])
        .applymap(highlight_numeric, subset=["Zero's Count", "Zero's Percentage"])  # Added highlight for numeric columns
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

    # Fetch a sample of the table to get the data types
    query = f"SELECT * FROM `{table_ref}` LIMIT 5"
    df_sample = client.query(query).to_dataframe()

    # Analyze the sample DataFrame to get data types
    data_types = df_sample.dtypes

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

# Save the combined HTML output to a single file with JavaScript and filter dropdown
html_output_path = f'DATA_SUMMARY_{timestamp}.html'
with open(html_output_path, 'w') as file:
    file.write(javascript_code)
    file.write(filter_dropdown)
    file.write(combined_html_output)

print(f"Combined HTML output with filter saved to {html_output_path}")
