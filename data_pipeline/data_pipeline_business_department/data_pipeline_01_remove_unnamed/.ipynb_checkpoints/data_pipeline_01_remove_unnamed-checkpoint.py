import pandas as pd
import numpy as np
import os
import sys

# Get the script's directory
script_directory = os.path.dirname(os.path.abspath(__file__))

# Set the working directory to the script's directory
os.chdir(script_directory)

# logs
sys.stdout = open('data_pipeline_operations_department_01_concat_data.out.log', 'w')
sys.stderr = open('data_pipeline_operations_department_01_concat_data.err.log', 'w')

#Load data
df_product = pd.read_excel("../../../data/business_department/product_list.xlsx")
print("Successfully loaded product list")

#Sorting
print("Data frame size before removing unnamed: ")
print(df_product.shape)
df_product = df_product.loc[:, ~df_product.columns.str.contains('^Unnamed')]
print("Unnamed columns successfully removed")
print("Data frame size after removing unnamed: ")
print(df_product.shape)

#save data
df_product.to_parquet("product_unnamed_removed.parquet")
print("Successfully saved data")