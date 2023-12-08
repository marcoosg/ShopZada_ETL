import pandas as pd
import numpy as np

# Loading the Data
df_campaign_data = pd.read_parquet(r"C:\Users\arian\final\data\enterprise_department\parquet\campaign_data_removed_symbols.parquet")
print("Succesfully Loaded the Data")

### Renaming discount to campaign_discount
df_campaign_data = df_campaign_data.rename(columns={'discount': 'campaign_discount'})
print("Succesfully Renamed Campaign Discount")

### Saving data
df_campaign_data.to_parquet(r"C:\Users\arian\final\data\enterprise_department\parquet\campaign_data_renamed.parquet")
print("\nSuccessfully Saved the Data!")
