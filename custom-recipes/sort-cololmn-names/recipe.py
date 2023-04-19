import dataiku
import pandas as pd, numpy as np

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


input_dataset_df = input_dataset.get_dataframe()

# -------------------------------------------------------------------------------- NOTEBOOK-CELL: CODE
sorted_columns_data_df = input_dataset_df[sorted(input_dataset_df.columns)]

# -------------------------------------------------------------------------------- NOTEBOOK-CELL: CODE
# Write recipe outputs
sorted_columns_data = dataiku.Dataset("sorted_columns_data")
sorted_columns_data.write_with_schema(sorted_columns_data_df)