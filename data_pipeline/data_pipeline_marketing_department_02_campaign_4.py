import pandas as pd
import numpy as np

# Loading the Data
df_campaign_data = pd.read_parquet(r"..\data\marketing_department\campaign_data_renamed.parquet")
print("Succesfully Loaded the Data")

### Making all Uppercase except Campaign_Description
df_campaign_data['campaign_name'] = df_campaign_data['campaign_name'].str.upper()
df_campaign_data['campaign_writer'] = df_campaign_data['campaign_writer'].str.upper()
print("Succesfully Loaded the Data")

### Uppercase to all column name
df_campaign_data.columns = df_campaign_data.columns.str.upper()

### Saving data
df_campaign_data.to_parquet(r"..\data\marketing_department\campaign_data_uppercase.parquet")
print("\nSuccessfully Saved the Data!")
