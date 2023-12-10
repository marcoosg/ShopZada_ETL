import pandas as pd
import numpy as np
import os
import sys

# Get the script's directory
script_directory = os.path.dirname(os.path.abspath(__file__))

# Set the working directory to the script's directory
os.chdir(script_directory)

# logs
sys.stdout = open('data_pipeline_marketing_department_03_transactional_campaign_column_uppercase.out.log', 'w')
sys.stderr = open('data_pipeline_marketing_department_03_transactional_campaign_column_uppercase.err.log', 'w')

# Loading the Data
df_transaction_campaign_data = pd.read_parquet(os.path.join('..', 'data_pipeline_marketing_department_02_campaign', 'transactional_camapaign_column.parquet'))
print("Succesfully Loaded the Data")

### Uppercase to all column name
df_transaction_campaign_data.columns = df_transaction_campaign_data.columns.str.upper()

### Making all uppercase 
df_transaction_campaign_data['ORDER_ID'] = df_transaction_campaign_data['ORDER_ID'].str.upper()
df_transaction_campaign_data['CAMPAIGN_ID'] = df_transaction_campaign_data['CAMPAIGN_ID'].str.upper()
df_transaction_campaign_data['ESTIMATED_ARRIVAL'] = df_transaction_campaign_data['ESTIMATED_ARRIVAL'].str.upper()

### Saving data
df_transaction_campaign_data.to_parquet('transactional_campaign_uppercase.parquet')
print("\nSuccessfully Saved the Data!")
