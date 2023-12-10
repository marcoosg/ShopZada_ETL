import pandas as pd
import numpy as np
import os
import sys

# Get the script's directory
script_directory = os.path.dirname(os.path.abspath(__file__))

# Set the working directory to the script's directory
os.chdir(script_directory)

# logs
sys.stdout = open('data_pipeline_marketing_department_01_transactional_campaign_rename.out.log', 'w')
sys.stderr = open('data_pipeline_marketing_department_01_transactional_campaign_rename.err.log', 'w')

# Loading the Data
df_transaction_campaign_data = pd.read_csv(os.path.join('..', '..', '..', 'data', 'marketing_department', 'transactional_campaign_data.csv'))
print("Successfully Loaded Transactional Campaign Data")

### Renaming column estimated arrival
df_transaction_campaign_data = df_transaction_campaign_data.rename(columns={'estimated arrival': 'estimated_arrival'})
print("Successfully Loaded Renemed estimated arrival")

### Converting transaction_date to date
df_transaction_campaign_data['transaction_date'] = pd.to_datetime(df_transaction_campaign_data['transaction_date'])
print("Succesfully convert transaction_date")

### Adding space between the number and days
df_transaction_campaign_data = df_transaction_campaign_data.assign(
    estimated_arrival = df_transaction_campaign_data['estimated_arrival'].str.extract(r'(\d+)') + " " + df_transaction_campaign_data['estimated_arrival'].str.extract(r'(\D+)')
    )
print("Succesfully addded space")

### Saving data
df_transaction_campaign_data.to_parquet('transactional_camapaign_renamed.parquet')
print("\nSuccessfully Saved the Data!")
