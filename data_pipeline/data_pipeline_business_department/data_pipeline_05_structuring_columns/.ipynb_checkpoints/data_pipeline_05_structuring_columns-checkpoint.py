import pandas as pd
import numpy as np
import os
import sys

# Get the script's directory
script_directory = os.path.dirname(os.path.abspath(__file__))

# Set the working directory to the script's directory
os.chdir(script_directory)

# logs
sys.stdout = open('data_pipeline_05_structuring_columns.out.log', 'w')
sys.stderr = open('data_pipeline_05_structuring_columns.err.log', 'w')

#Load data
df_product = pd.read_parquet("../data_pipeline_04_type_changing/product_type_changes.parquet")
print("Successfully loaded the data")

#renaming columns
df_product = df_product.rename(columns={'price':'product_price'})
df_product['product_type'] = df_product['product_type'].str.upper()
df_product['product_name'] = df_product['product_name'].str.upper()
# Uppercase all columns
df_product.columns = df_product.columns.str.upper()
print(df_product.head(30))

#Saving data
df_product.to_parquet("product_column_structure.parquet")
df_product.to_parquet("product_final_list.parquet")
print("Successfully saved data")