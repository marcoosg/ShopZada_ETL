import pandas as pd
import numpy as np
import os
import sys

# Get the script's directory
script_directory = os.path.dirname(os.path.abspath(__file__))

# Set the working directory to the script's directory
os.chdir(script_directory)

# logs
sys.stdout = open('data_pipeline_operations_department_05_converted_dtype.out.log', 'w')
sys.stderr = open('data_pipeline_operations_department_05_converted_dtype.err.log', 'w')

df_converted_data_type = pd.read_parquet('../data_pipeline_operations_department_04_uppercased/data_pipeline_operations_department_04_uppercased.parquet')
print("Succesfuly Loaded Data")

# Convert TRANSACTION_DATE to datetime format
df_converted_data_type['TRANSACTION_DATE'] = pd.to_datetime(df_converted_data_type['TRANSACTION_DATE'])
print("Succesfuly Converted Column to Proper Data ")

## Saving data
df_converted_data_type.to_parquet("data_pipeline_operations_department_05_converted_dtype.parquet")
print("Succesfully Saved Data")