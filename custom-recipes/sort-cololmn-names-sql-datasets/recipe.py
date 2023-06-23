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

my_table = input_dataset.project_key + "_" + input_dataset.short_name


schema = input_dataset.read_schema()
columns = [col["name"] for col in schema]
sorted_cols = sorted(columns)

query = 'SELECT ' + ', '.join(f'"{c}"' for c in sorted_cols) + f' FROM "{my_table}"'

# create an executor and pass to it the dataset so the executor knows which SQL database to target
executor = SQLExecutor2(dataset=input_dataset)

# create a dataframe using the query
sorted_df = executor.query_to_df(
    """
     %s
    """ % (query))

sorted_df

# -------------------------------------------------------------------------------- NOTEBOOK-CELL: CODE
# Write recipe outputs
output_dataset.write_with_schema(sorted_df)