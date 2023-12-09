import pandas as pd
import numpy as np
import os
import sys

# Get the script's directory
script_directory = os.path.dirname(os.path.abspath(__file__))

# Set the working directory to the script's directory
os.chdir(script_directory)

# logs
sys.stdout = open('data_pipeline_marketing_department_02_campaign_data_uppercase.out.log', 'w')
sys.stderr = open('data_pipeline_marketing_department_02_campaign_data_uppercase.err.log', 'w')
# Loading the Data
df_campaign_data = pd.read_parquet('../data_pipeline_marketing_department_01_campaign/campaign_data_separated.parquet')
print("Successfully Loaded the Data")

### Making all Uppercase except Campaign_Description
df_campaign_data['campaign_name'] = df_campaign_data['campaign_name'].str.upper()
df_campaign_data['campaign_writer'] = df_campaign_data['campaign_writer'].str.upper()
print("Successfully Loaded the Data")

### Uppercase to all column name
df_campaign_data.columns = df_campaign_data.columns.str.upper()

### Saving data
df_campaign_data.to_parquet('campaign_data_uppercase.parquet')
print("\nSuccessfully Saved the Data!")
