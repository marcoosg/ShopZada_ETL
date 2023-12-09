import pandas as pd
import numpy as np
import os
import sys

# Get the script's directory
script_directory = os.path.dirname(os.path.abspath(__file__))

# Set the working directory to the script's directory
os.chdir(script_directory)

# logs
sys.stdout = open('data_pipeline_operations_department_06_corrected_product.out.log', 'w')
sys.stderr = open('data_pipeline_operations_department_06_corrected_product.err.log', 'w')

df_converted_data_type = pd.read_parquet('../data_pipeline_operations_department_05_converted_dtype/data_pipeline_operations_department_05_converted_dtype.parquet')
print("Succesfuly Loaded Data")

# get product_list
df_product_list = pd.read_parquet('../product_final_list.parquet')
df_product_list.columns = df_product_list.columns.str.upper()

# Merge the dataframes on the 'PRODUCT_ID' column
merged_df = pd.merge(df_converted_data_type, df_product_list[['PRODUCT_ID', 'PRICE', 'PRODUCT_NAME']], on='PRODUCT_ID', how='left', suffixes=('_converted', '_original'))

# Update the 'PRICE' column in df_converted_data_type with the values from df_product_list
merged_df['PRICE_converted'] = merged_df['PRICE_original']

# Update the 'PRODUCT_NAME' column in df_converted_data_type with the values from df_product_list
merged_df['PRODUCT_NAME_converted'] = merged_df['PRODUCT_NAME_original']

# Drop rows with NaN values in 'PRODUCT_NAME_converted' and 'PRODUCT_ID'
merged_df.dropna(subset=['PRODUCT_NAME_converted', 'PRODUCT_ID'], inplace=True)

# Drop unnecessary columns
merged_df = merged_df.drop(['PRICE_original', 'PRODUCT_NAME_original'], axis=1)

# Rename the 'PRICE_converted' column to 'PRICE'
merged_df = merged_df.rename(columns={'PRICE_converted': 'PRICE'})

# Rename the 'PRODUCT_NAME_converted' column to 'PRODUCT_NAME'
merged_df = merged_df.rename(columns={'PRODUCT_NAME_converted': 'PRODUCT_NAME'})

df_corrected_product = merged_df.copy()
print("Succesfuly Corrected Product Existence, Name, and Price")

## Saving data
df_corrected_product.to_parquet("data_pipeline_operations_department_06_corrected_product.parquet")
print("Succesfully Saved Data")



