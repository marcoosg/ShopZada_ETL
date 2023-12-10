import pandas as pd
import numpy as np
import os
import sys

# Get the script's directory
script_directory = os.path.dirname(os.path.abspath(__file__))

# Set the working directory to the script's directory
os.chdir(script_directory)

# logs
sys.stdout = open('data_pipeline_operations_department_02_merged_data.out.log', 'w')
sys.stderr = open('data_pipeline_operations_department_02_merged_data.err.log', 'w')

df_line_item_data_prices_concat = pd.read_parquet(os.path.join(script_directory, '..', 'data_pipeline_operations_department_01_concat_data', 'line_item_data_prices_concat.parquet'))

df_line_item_data_products_concat = pd.read_parquet(os.path.join(script_directory, '..', 'data_pipeline_operations_department_01_concat_data', 'line_item_data_products_concat.parquet'))

df_order_data_concat = pd.read_parquet(os.path.join(script_directory, '..', 'data_pipeline_operations_department_01_concat_data', 'order_data_concat.parquet'))

df_order_delays = pd.read_parquet(os.path.join(script_directory, '..', 'data_pipeline_operations_department_01_concat_data', 'order_delays.parquet'))
print("Succesfuly Loaded Data")

df_merge_orderData_lineProducts = pd.merge(df_order_data_concat, df_line_item_data_products_concat, on='ORDER_ID')
print("Succesfuly Merged order_data and line_item_data_products")
df_merge_orderData_linePrices = pd.merge(df_merge_orderData_lineProducts, df_line_item_data_prices_concat, on='ORDER_ID')
print("Succesfuly Merged order_data and line_item_data_prices")
df_merge_orderData_orderDelays = pd.merge(df_merge_orderData_linePrices, df_order_delays, on='ORDER_ID', how='left')
df_merge_orderData_orderDelays['DELAY IN DAYS'].fillna(0, inplace=True)
df_merge_orderData_orderDelays['DELAY IN DAYS'] = df_merge_orderData_orderDelays['DELAY IN DAYS'].astype(int)
print("Succesfuly Merged order_data and order_delays")
df_merge_operations = df_merge_orderData_orderDelays.copy()

## Saving data
df_merge_operations.to_parquet("data_pipeline_operations_department_02_merged_data.parquet")
print("Succesfully Saved Data")