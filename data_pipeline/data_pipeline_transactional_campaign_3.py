import pandas as pd
import numpy as np

# Loading the Data
df_transaction_campaign_data = pd.read_parquet(r"..\data\marketing_department\parquet\transactional_camapaign_column.parquet")
print("Succesfully Loaded the Data")

### Uppercase to all column name
df_transaction_campaign_data.columns = df_transaction_campaign_data.columns.str.upper()

### Making all uppercase 
df_transaction_campaign_data['ORDER_ID'] = df_transaction_campaign_data['ORDER_ID'].str.upper()
df_transaction_campaign_data['CAMPAIGN_ID'] = df_transaction_campaign_data['CAMPAIGN_ID'].str.upper()
df_transaction_campaign_data['ESTIMATED_ARRIVAL'] = df_transaction_campaign_data['ESTIMATED_ARRIVAL'].str.upper()

### Saving data
df_transaction_campaign_data.to_parquet(r"...\data\marketing_department\parquet\transactional_campaign_uppercase.parquet")
print("\nSuccessfully Saved the Data!")