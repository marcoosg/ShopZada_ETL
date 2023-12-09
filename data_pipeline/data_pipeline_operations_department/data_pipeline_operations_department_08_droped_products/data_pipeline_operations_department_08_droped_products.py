import pandas as pd
import numpy as np
import os
import sys

# Get the script's directory
script_directory = os.path.dirname(os.path.abspath(__file__))

# Set the working directory to the script's directory
os.chdir(script_directory)

# logs
sys.stdout = open('data_pipeline_operations_department_08_droped_products.out.log', 'w')
sys.stderr = open('data_pipeline_operations_department_08_droped_products.err.log', 'w')

# Load data
df_droped_duplicates = pd.read_parquet('../data_pipeline_operations_department_07_droped_duplicates/data_pipeline_operations_department_07_droped_duplicates.parquet')
df_transactional_campaign = pd.read_parquet('../../data_pipeline_marketing_department/data_pipeline_marketing_department_03_campaign/transactional_campaign_uppercase.parquet')
print("Succesfuly Loaded Data")

# Drop Duplicate Product in an Order and keep last
df_drop_duplicate_products = df_droped_duplicates.drop_duplicates(subset=['ORDER_ID','PRODUCT_ID'], keep='last')
df_drop_duplicate_products.reset_index(drop=True, inplace=True)
print("Succesfuly Dropped Duplicate Products in an Order")

## Saving data
df_drop_duplicate_products.to_parquet("data_pipeline_operations_department_08_droped_products.parquet")
print("Succesfully Saved Data")
