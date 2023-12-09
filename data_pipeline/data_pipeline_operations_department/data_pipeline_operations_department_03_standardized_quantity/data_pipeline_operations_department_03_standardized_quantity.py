import pandas as pd
import numpy as np
import os
import sys

# Get the script's directory
script_directory = os.path.dirname(os.path.abspath(__file__))

# Set the working directory to the script's directory
os.chdir(script_directory)

# logs
sys.stdout = open('data_pipeline_operations_department_03_standardized_quantity.out.log', 'w')
sys.stderr = open('data_pipeline_operations_department_03_standardized_quantity.err.log', 'w')

df_standardized_quantity = pd.read_parquet('../data_pipeline_operations_department_02_merged_data/data_pipeline_operations_department_02_merged_data.parquet')
print("Succesfuly Loaded Data")

df_standardized_quantity['QUANTITY'] = df_standardized_quantity['QUANTITY'].astype(str).str.extract(r'(\d+)', expand=False).astype(float).astype('Int64')
print("Succesfuly Standardized QUANTITY")

## Saving data
df_standardized_quantity.to_parquet("data_pipeline_operations_department_03_standardized_quantity.parquet")
print("Succesfully Saved Data")

