import pandas as pd
import numpy as np
import os
import sys

# Get the script's directory
script_directory = os.path.dirname(os.path.abspath(__file__))

# Set the working directory to the script's directory
os.chdir(script_directory)

# logs
sys.stdout = open('data_pipeline_07_fix_column_order.out.log', 'w')
sys.stderr = open('data_pipeline_07_fix_column_order.err.log', 'w')

#load data
df_merged_data=pd.read_parquet('../data_pipeline_06_fix_column_name/fixed_column_name.parquet')
print("Successfully loaded data")

print(df_merged_data.iloc[3000:4000])

#fix column order
df_merged_data = df_merged_data[['USER_ID', 'USER_CREATION_DATE', 'USER_NAME', 'USER_ADDR_STREET', 'USER_ADDR_STATE', 'USER_ADDR_CITY', 'USER_ADDR_COUNTRY', 'USER_BIRTHDATE','USER_GENDER', 'USER_DEVICE_ADDR', 'USER_TYPE', 'USER_JOB_TITLE', 'USER_JOB_LVL' ,'USER_CC_NO', 'USER_ISSUING_BANK']]
print("Successfully fixed column order")
print(df_merged_data.iloc[3000:4000])

#save data
df_merged_data.to_parquet("column_order.parquet")

