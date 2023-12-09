import pandas as pd
import numpy as np

#load data
df_merged_data=pd.read_parquet("removed_duplicates.parquet")
print("Successfully loaded data")

#converting objects to date
df_merged_data['birthdate'] = pd.to_datetime(df_merged_data['birthdate']).dt.date 
#removed time 
df_merged_data['creation_date'] = pd.to_datetime(df_merged_data['creation_date']).dt.date 
#removed time
print("Successfully converted objects to date")

#save data
df_merged_data.to_parquet("object_to_date.parquet")
print("Successfully saved data")