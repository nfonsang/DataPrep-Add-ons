# Code for custom code recipe sort-cololmn-names-sql-datasets (imported from a Python recipe)

# To finish creating your custom recipe from your original PySpark recipe, you need to:
#  - Declare the input and output roles in recipe.json
#  - Replace the dataset names by roles access in your code
#  - Declare, if any, the params of your custom recipe in recipe.json
#  - Replace the hardcoded params values by acccess to the configuration map

# See sample code below for how to do that.
# The code of your original recipe is included afterwards for convenience.
# Please also see the "recipe.json" file for more information.

# import the classes for accessing DSS objects from the recipe
import dataiku
# Import the helpers for custom recipes
from dataiku.customrecipe import get_input_names_for_role
from dataiku.customrecipe import get_output_names_for_role
from dataiku.customrecipe import get_recipe_config

# Inputs and outputs are defined by roles. In the recipe's I/O tab, the user can associate one
# or more dataset to each input and output role.
# Roles need to be defined in recipe.json, in the inputRoles and outputRoles fields.

# To  retrieve the datasets of an input role named 'input_A' as an array of dataset names:
input_A_names = get_input_names_for_role('input_A_role')
# The dataset objects themselves can then be created like this:
input_A_datasets = [dataiku.Dataset(name) for name in input_A_names]

# For outputs, the process is the same:
output_A_names = get_output_names_for_role('main_output')
output_A_datasets = [dataiku.Dataset(name) for name in output_A_names]


# The configuration consists of the parameters set up by the user in the recipe Settings tab.

# Parameters must be added to the recipe.json file so that DSS can prompt the user for values in
# the Settings tab of the recipe. The field "params" holds a list of all the params for wich the
# user will be prompted for values.

# The configuration is simply a map of parameters, and retrieving the value of one of them is simply:
my_variable = get_recipe_config()['parameter_name']

# For optional parameters, you should provide a default value in case the parameter is not present:
my_variable = get_recipe_config().get('parameter_name', None)

# Note about typing:
# The configuration of the recipe is passed through a JSON object
# As such, INT parameters of the recipe are received in the get_recipe_config() dict as a Python float.
# If you absolutely require a Python int, use int(get_recipe_config()["my_int_param"])


#############################
# Your original recipe
#############################

# -------------------------------------------------------------------------------- NOTEBOOK-CELL: CODE
# -*- coding: utf-8 -*-
import dataiku
import pandas as pd, numpy as np
from dataiku import pandasutils as pdu
from dataiku import SQLExecutor2

# -------------------------------------------------------------------------------- NOTEBOOK-CELL: CODE
# Read recipe inputs
car_email_data_copy = dataiku.Dataset("car_email_data_copy")

# -------------------------------------------------------------------------------- NOTEBOOK-CELL: CODE
my_table = car_email_data_copy.project_key + "_" + car_email_data_copy.short_name
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