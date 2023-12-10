import pandas as pd
import numpy as np
import lxml
import html5lib
import phonenumbers as pn
import os
import sys

# Get the script's directory
script_directory = os.path.dirname(os.path.abspath(__file__))

# Set the working directory to the script's directory
os.chdir(script_directory)

# logs
sys.stdout = open('data_pipeline_enterprise_department_merchant_data_02_remove_duplicates.out.log', 'w')
sys.stderr = open('data_pipeline_enterprise_department_merchant_data_02_remove_duplicates.err.log', 'w')

#load data
df_merchant = pd.read_parquet(os.path.join('..', 'data_pipeline_enterprise_department_merchant_data_01_convert_to_date', 'convert_to_date.parquet'))
print("Successfully loaded data")

#sort values
df_merchant = df_merchant.sort_values(by='creation_date')

#remove duplicates
print("Number of records before removing duplicates:")
print(df_merchant.shape)
df_merchant.drop_duplicates()
df_merchant = df_merchant.drop_duplicates(subset=['merchant_id'],keep='last')
del df_merchant['Unnamed: 0']
print("Duplicates successfully removed")
print("Number of records after removing duplicates:")
print(df_merchant.shape)

#save data
df_merchant.to_parquet("duplicates_removed.parquet")
print("Successfully saved data")
