import pandas as pd
import numpy as np
import os
import sys

# Get the script's directory
script_directory = os.path.dirname(os.path.abspath(__file__))

# Set the working directory to the script's directory
os.chdir(script_directory)

# logs
sys.stdout = open('data_pipeline_marketing_department_02_campaign_remove_symbols.out.log', 'w')
sys.stderr = open('data_pipeline_marketing_department_02_campaign_remove_symbols.err.log', 'w')

# Loading the Data
df_campaign_data = pd.read_parquet('../data_pipeline_marketing_department_01_campaign/campaign_data_separated.parquet')
print("Successfully Loaded the Data")

### Removing unnecessary symbols
#cleaning campaign description by removing unnecessary symbols
df_campaign_data['campaign_description'] = df_campaign_data['campaign_description'].str.replace('"', '')
df_campaign_data['campaign_writer'] = df_campaign_data['campaign_writer'].str.replace('"', '')
print("Successfully removed unnecessary symbols")

#cleaning structure in discount column
df_campaign_data['discount'] = df_campaign_data['discount'].str.replace('%%', '%')
df_campaign_data['discount'] = df_campaign_data['discount'].str.replace('pct', '%')
df_campaign_data['discount'] = df_campaign_data['discount'].str.replace('percent', '%')
print("Successfully cleaned discount column structure")

### Saving data
df_campaign_data.to_parquet('campaign_data_removed_symbols.parquet')
print("\nSuccessfully Saved the Data!")
