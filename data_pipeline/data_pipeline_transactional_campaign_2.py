import pandas as pd
import numpy as np

# Loading the Data
df_transaction_campaign_data = pd.read_parquet(r"..\data\marketing_department\parquet\transactional_camapaign_renamed.parquet")
print("Succesfully Loaded the Data")

### Remove unnamed column
df_transaction_campaign_data.drop('Unnamed: 0', inplace=True, axis=1)
print("Succesfully removed unnamed")

### Change order of column
df_transaction_campaign_data = df_transaction_campaign_data[['order_id', 'campaign_id', 'transaction_date', 'estimated_arrival', 'availed']]
print("Succesfully changed column")

### Saving data
df_transaction_campaign_data.to_parquet(r"..\data\marketing_department\parquet\transactional_camapaign_column.parquet")
print("\nSuccessfully Saved the Data!")