import pandas as pd
import numpy as np

#load data
df_merged_data=pd.read_parquet("object_to_date.parquet")
print("Successfully loaded data")

# upper cased cell
df_merged_data['issuing_bank'] = df_merged_data['issuing_bank'].str.upper()
df_merged_data['street'] = df_merged_data['street'].str.upper()
df_merged_data['state'] = df_merged_data['state'].str.upper()
df_merged_data['city'] = df_merged_data['city'].str.upper()
df_merged_data['country'] = df_merged_data['country'].str.upper()
df_merged_data['gender'] = df_merged_data['gender'].str.upper()
df_merged_data['user_type'] = df_merged_data['user_type'].str.upper()
df_merged_data['job_title'] = df_merged_data['job_title'].str.upper()
df_merged_data['job_level'] = df_merged_data['job_level'].str.upper()
df_merged_data['name'] = df_merged_data['name'].str.upper()
print("Info uppercased")
print(df_merged_data.head())

#save data
df_merged_data.to_parquet("uppercased_info.parquet")