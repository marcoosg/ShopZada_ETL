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
df_merged_data=pd.read_parquet(os.path.join('..', 'data_pipeline_06_fix_column_name', 'fixed_column_name.parquet'))
print("Successfully loaded data")

print(df_merged_data.iloc[3000:4000])

#fix column order
df_merged_data = df_merged_data[['USER_ID', 'USER_CREATION_DATE', 'USER_NAME', 'USER_ADDR_STREET', 'USER_ADDR_STATE', 'USER_ADDR_CITY', 'USER_ADDR_COUNTRY', 'USER_BIRTHDATE','USER_GENDER', 'USER_DEVICE_ADDR', 'USER_TYPE', 'USER_JOB_TITLE', 'USER_JOB_LVL' ,'USER_CC_NO', 'USER_ISSUING_BANK']]
print("Successfully fixed column order")
print(df_merged_data.iloc[3000:4000])

#save data
df_merged_data.to_parquet("column_order.parquet")

df_user_merged = df_merged_data.copy()
# Create df_user
df_user = df_user_merged[['USER_ID', 'USER_CREATION_DATE', 'USER_NAME', 'USER_ADDR_STREET', 'USER_ADDR_STATE',
                          'USER_ADDR_CITY', 'USER_ADDR_COUNTRY', 'USER_BIRTHDATE', 'USER_GENDER', 'USER_DEVICE_ADDR',
                          'USER_TYPE']].copy()

# Rename columns for clarity
df_user.columns = ['USER_ID', 'CREATION_DATE', 'NAME', 'STREET', 'STATE', 'CITY', 'COUNTRY', 'BIRTHDATE', 'GENDER',
                   'DEVICE_ADDRESS', 'USER_TYPE']

# # Set USER_ID as the primary key
# df_user.set_index('USER_ID', inplace=True)

# Create df_user_job
df_user_job = df_user_merged[['USER_ID', 'USER_NAME', 'USER_JOB_TITLE', 'USER_JOB_LVL']].copy()

# Rename columns for clarity
df_user_job.columns = ['USER_ID', 'NAME', 'JOB_TITLE', 'JOB_LEVEL']

# # Set USER_ID as the foreign key
# df_user_job.set_index('USER_ID', inplace=True)

# Create df_user_credit_card
df_user_credit_card = df_user_merged[['USER_ID', 'USER_NAME', 'USER_CC_NO', 'USER_ISSUING_BANK']].copy()

# Rename columns for clarity
df_user_credit_card.columns = ['USER_ID', 'NAME', 'CREDIT_CARD_NUMBER', 'ISSUING_BANK']

# # Set USER_ID as the foreign key
# df_user_credit_card.set_index('USER_ID', inplace=True)

df_user.to_parquet("user.parquet")
df_user_credit_card.to_parquet("user_credit_card.parquet")
df_user_job.to_parquet("user_job.parquet")

