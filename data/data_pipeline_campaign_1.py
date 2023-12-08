import pandas as pd
import numpy as np

# Loading the Data
df_campaign_data = pd.read_csv('../marketing_department/campaign_data.csv')
print("Succesfully Loaded Campaign Data")

### Separate into different columns
df_campaign_data[['Unnamed', 'campaign_id' , 'campaign_name', 'campaign_description', 'discount']] = df_campaign_data['\tcampaign_id\tcampaign_name\tcampaign_description\tdiscount'].str.split('\t', expand=True)
df_campaign_data.drop('Unnamed', inplace=True, axis=1)
df_campaign_data.drop('\tcampaign_id\tcampaign_name\tcampaign_description\tdiscount', inplace=True, axis=1)
print("Succesfully Separated into different columns")

### Separate campaign description to another column campaign_writer
df_campaign_data[['campaign_description', 'campaign_writer']] = df_campaign_data['campaign_description'].str.split(" - ", expand=True)
print("Succesfully Separated campaign writer")

### Saving data
df_campaign_data.to_parquet(r"C:\Users\arian\final\data\enterprise_department\parquet\campaign_data_separated.parquet")
print("\nSuccessfully Saved the Data!")

