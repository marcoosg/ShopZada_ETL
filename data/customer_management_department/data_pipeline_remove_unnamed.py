import pandas as pd
import numpy as np

#load data
df_merged_data=pd.read_parquet("fixed_job_title.parquet")
print("Successfully loaded data")

#removing unnamed 
df_merged_data.drop('Unnamed: 0', inplace=True, axis=1)
print ("Unnamed removed")
print(df_merged_data.shape)

#save data
df_merged_data.to_parquet("removed_unnamed.parquet")
print("Successfully saved data")