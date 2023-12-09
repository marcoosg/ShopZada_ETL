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
sys.stdout = open('data_pipeline_enterprise_department_order_with_merchant_01_uppercase columns.out.log', 'w')
sys.stderr = open('data_pipeline_enterprise_department_order_with_merchant_01_uppercase columns.err.log', 'w')

#load data
df_data1 = pd.read_parquet('../../../data/enterprise_department/order_with_merchant_data1.parquet')
df_data2 = pd.read_parquet('../../../data/enterprise_department/order_with_merchant_data2.parquet')
df_data3 = pd.read_csv('../../../data/enterprise_department/order_with_merchant_data3.csv')
print("Successfully loaded data")

#concat data
appended_df = pd.concat([df_data1, df_data2, df_data3], ignore_index=True)
print("Successfully concatinated data")

#delete columns with 0 data
del appended_df['Unnamed: 0']

#uppercase columns
appended_df.columns = appended_df.columns.str.upper()
print("Successfully uppercased columns")
print(appended_df.info())

#save data
appended_df.to_parquet("fixed_order_with_merchant.parquet")
print("Successfully saved data")