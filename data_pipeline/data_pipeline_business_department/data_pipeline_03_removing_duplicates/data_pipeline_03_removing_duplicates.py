import pandas as pd
import numpy as np
import os
import sys

# Get the script's directory
script_directory = os.path.dirname(os.path.abspath(__file__))

# Set the working directory to the script's directory
os.chdir(script_directory)

# logs
sys.stdout = open('data_pipeline_03_removing_duplicates.out.log', 'w')
sys.stderr = open('data_pipeline_03_removing_duplicates.err.log', 'w')

#Load data
df_product = pd.read_parquet(os.path.join('..', 'data_pipeline_02_merging_product', 'product_type_merged.parquet'))
print("Successfully loaded the data")

#removing same product name and product type, keeping only the newly added data
df_product = df_product.drop_duplicates(subset=["product_name","product_type"], keep="last")
print(df_product.shape)

#removing product id, keeping only the newly added data (there may be some changes with product ids)
df_product = df_product.drop_duplicates(subset=['product_id'], keep='last')
print(df_product.shape)
print(df_product.nunique())

#Saving the data
df_product.to_parquet("product_duplicates_removed.parquet")
print("Successfully removed duplicates")