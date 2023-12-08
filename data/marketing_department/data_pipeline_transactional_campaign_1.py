import pandas as pd
import numpy as np

# Loading the Data
df_transaction_campaign_data = pd.read_csv('../marketing_department/transactional_campaign_data.csv')
print("Succesfully Loaded Transactional Campaign Data")

### Renaming column estimated arrival
df_transaction_campaign_data = df_transaction_campaign_data.rename(columns={'estimated arrival': 'estimated_arrival'})
print("Succesfully Loaded Renemed estimated arrival")

### Converting transaction_date to date
df_transaction_campaign_data['transaction_date'] = pd.to_datetime(df_transaction_campaign_data['transaction_date'])
print("Succesfully convert transaction_date")

### Adding space between the number and days
df_transaction_campaign_data = df_transaction_campaign_data.assign(
    estimated_arrival = df_transaction_campaign_data['estimated_arrival'].str.extract(r'(\d+)') + " " + df_transaction_campaign_data['estimated_arrival'].str.extract(r'(\D+)')
    )
print("Succesfully addded space")

### Saving data
df_transaction_campaign_data.to_parquet(r"C:\Users\arian\final\data\enterprise_department\parquet\transactional_camapaign_renamed.parquet")
print("\nSuccessfully Saved the Data!")
