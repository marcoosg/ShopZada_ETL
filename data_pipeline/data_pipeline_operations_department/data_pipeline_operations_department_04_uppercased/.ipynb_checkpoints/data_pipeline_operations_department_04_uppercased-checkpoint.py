import pandas as pd
import numpy as np
import os
import sys

# Get the script's directory
script_directory = os.path.dirname(os.path.abspath(__file__))

# Set the working directory to the script's directory
os.chdir(script_directory)

# logs
sys.stdout = open('data_pipeline_operations_department_04_uppercased.out.log', 'w')
sys.stderr = open('data_pipeline_operations_department_04_uppercased.err.log', 'w')

df_uppercased = pd.read_parquet(os.path.join('..', 'data_pipeline_operations_department_03_standardized_quantity', 'data_pipeline_operations_department_03_standardized_quantity.parquet'))
print("Succesfuly Loaded Data")

# Add _ to column names with spaces
df_uppercased = df_uppercased.rename(columns={'ESTIMATED ARRIVAL': 'ESTIMATED_ARRIVAL'})
df_uppercased = df_uppercased.rename(columns={'DELAY IN DAYS': 'DELAY_IN_DAYS'})
# Uppercase all strings
df_uppercased = df_uppercased.apply(lambda x: x.str.upper() if x.dtype == 'O' else x)
# Adding space in between the number and days in ESTIMATED_ARRIVAL
df_uppercased = df_uppercased.assign(
    ESTIMATED_ARRIVAL = df_uppercased['ESTIMATED_ARRIVAL'].str.extract(r'(\d+)') + " " + df_uppercased['ESTIMATED_ARRIVAL'].str.extract(r'(\D+)')
   )
df_uppercased.head()
print("Succesfuly Uppercased All Strings and Standardized Column Names")

## Saving data
df_uppercased.to_parquet("data_pipeline_operations_department_04_uppercased.parquet")
print("Succesfully Saved Data")


