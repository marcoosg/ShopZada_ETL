import pandas as pd
import numpy as np

# Loading the Data
df_campaign_data = pd.read_parquet(r"C:\Users\arian\final\data\enterprise_department\parquet\campaign_data_separated.parquet")
print("Succesfully Loaded the Data")

### Removing unnecessary symbols
#cleaning campaign description by removing unnecessary symbols
df_campaign_data['campaign_description'] = df_campaign_data['campaign_description'].str.replace('"', '')
df_campaign_data['campaign_writer'] = df_campaign_data['campaign_writer'].str.replace('"', '')
print("Succesfully removed unnecessary symbols")

#cleaning structure in discount column
df_campaign_data['discount'] = df_campaign_data['discount'].str.replace('%%', '%')
df_campaign_data['discount'] = df_campaign_data['discount'].str.replace('pct', '%')
df_campaign_data['discount'] = df_campaign_data['discount'].str.replace('percent', '%')
print("Succesfully cleaned discount column structure")

### Saving data
df_campaign_data.to_parquet(r"C:\Users\arian\final\data\enterprise_department\parquet\campaign_data_removed_symbols.parquet")
print("\nSuccessfully Saved the Data!")
