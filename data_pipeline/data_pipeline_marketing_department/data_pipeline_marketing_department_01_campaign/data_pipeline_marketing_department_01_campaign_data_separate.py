import pandas as pd
import numpy as np
import os
import sys

# Get the script's directory
script_directory = os.path.dirname(os.path.abspath(__file__))

# Set the working directory to the script's directory
os.chdir(script_directory)

# logs
sys.stdout = open('data_pipeline_marketing_department_01_campaign_data_separate.out.log', 'w')
sys.stderr = open('data_pipeline_marketing_department_01_campaign_data_separate.err.log', 'w')

# Loading the Data
df_campaign_data = pd.read_csv('../../../data/marketing_department/campaign_data.csv')
print("Successfully Loaded Campaign Data")

### Separate into different columns
df_campaign_data[['Unnamed', 'campaign_id' , 'campaign_name', 'campaign_description', 'discount']] = df_campaign_data['\tcampaign_id\tcampaign_name\tcampaign_description\tdiscount'].str.split('\t', expand=True)
df_campaign_data.drop('Unnamed', inplace=True, axis=1)
df_campaign_data.drop('\tcampaign_id\tcampaign_name\tcampaign_description\tdiscount', inplace=True, axis=1)
print("Successfully Separated into different columns")

### Separate campaign description to another column campaign_writer
df_campaign_data[['campaign_description', 'campaign_writer']] = df_campaign_data['campaign_description'].str.split(" - ", expand=True)
print("Successfully Separated campaign writer")

#Rename discount to campaign discount
df_campaign_data = df_campaign_data.rename(columns={'discount': 'campaign_discount'})

### Saving data
df_campaign_data.to_parquet('campaign_data_separated.parquet')
print("\nSuccessfully Saved the Data!")

