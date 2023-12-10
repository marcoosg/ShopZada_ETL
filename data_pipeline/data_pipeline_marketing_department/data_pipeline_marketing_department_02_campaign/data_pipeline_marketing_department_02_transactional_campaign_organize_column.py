import pandas as pd
import numpy as np
import os
import sys

# Get the script's directory
script_directory = os.path.dirname(os.path.abspath(__file__))

# Set the working directory to the script's directory
os.chdir(script_directory)

# logs
sys.stdout = open('data_pipeline_marketing_department_02_transactional_campaign_organize_column.out.log', 'w')
sys.stderr = open('data_pipeline_marketing_department_02_transactional_campaign_organize_column.err.log', 'w')

# Loading the Data
df_transaction_campaign_data = pd.read_parquet(os.path.join('..', 'data_pipeline_marketing_department_01_campaign', 'transactional_camapaign_renamed.parquet'))
print("Successfully Loaded the Data")

### Remove unnamed column
df_transaction_campaign_data.drop('Unnamed: 0', inplace=True, axis=1)
print("Successfully removed unnamed")

### Change the order of column
df_transaction_campaign_data = df_transaction_campaign_data[['order_id', 'campaign_id', 'transaction_date', 'estimated_arrival', 'availed']]
print("Successfully changed column")

### Saving data
df_transaction_campaign_data.to_parquet('transactional_camapaign_column.parquet')
print("\nSuccessfully Saved the Data!")
