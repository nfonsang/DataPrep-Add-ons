import dataiku
import pandas as pd, numpy as np
from dataiku import pandasutils as pdu
from dataiku import SQLExecutor2

# Import the helpers for custom recipes
from dataiku.customrecipe import get_input_names_for_role
from dataiku.customrecipe import get_output_names_for_role
from dataiku.customrecipe import get_recipe_config

# Get input datasets
input_dataset_name = get_input_names_for_role('input_dataset')[0]
input_dataset = dataiku.Dataset(input_dataset_name)

# Get input output datasets
output_dataset_name = get_output_names_for_role('output_dataset')[0]
output_dataset = dataiku.Dataset(output_dataset_name)


# -------------------------------------------------------------------------------- NOTEBOOK-CELL: CODE
my_table = input_dataset_copy.project_key + "_" + input_dataset.short_name
my_table

# -------------------------------------------------------------------------------- NOTEBOOK-CELL: CODE
#dir(car_email_data_copy)

# -------------------------------------------------------------------------------- NOTEBOOK-CELL: CODE
schema = car_email_data_copy.read_schema()
columns = [col["name"] for col in schema]
sorted_cols = sorted(columns)

# -------------------------------------------------------------------------------- NOTEBOOK-CELL: CODE
query = 'SELECT ' + ', '.join(f'"{c}"' for c in sorted_cols) + f' FROM "{my_table}"'
print(query)

# -------------------------------------------------------------------------------- NOTEBOOK-CELL: CODE
# create an executor and pass to it the dataset so the executor knows which SQL database to target
executor = SQLExecutor2(dataset=car_email_data_copy)

# -------------------------------------------------------------------------------- NOTEBOOK-CELL: CODE
# create a dataframe using the query
sorted_df = executor.query_to_df(
    """
     %s
    """ % (query))

sorted_df

# -------------------------------------------------------------------------------- NOTEBOOK-CELL: CODE
# Write recipe outputs
SQL_execution_output = dataiku.Dataset("SQL_execution_output")
SQL_execution_output.write_with_schema(sorted_df)