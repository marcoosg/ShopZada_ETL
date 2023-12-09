import pandas as pd
import numpy as np
import os
import sys

# Get the script's directory
script_directory = os.path.dirname(os.path.abspath(__file__))

# Set the working directory to the script's directory
os.chdir(script_directory)

# logs
sys.stdout = open('data_pipeline_operations_department_07_droped_duplicates.out.log', 'w')
sys.stderr = open('data_pipeline_operations_department_07_droped_duplicates.err.log', 'w')

df_corrected_product = pd.read_parquet('../data_pipeline_operations_department_06_corrected_product/data_pipeline_operations_department_06_corrected_product.parquet')
print("Succesfuly Loaded Data")

# Drop duplicates
df_droped_duplicates = df_corrected_product.drop_duplicates()
print("Succesfuly Dropped Duplicates")

## Saving data
df_droped_duplicates.to_parquet("data_pipeline_operations_department_07_droped_duplicates.parquet")
print("Succesfully Saved Data")