import pandas as pd
import numpy as np

#load data
df_merged_data=pd.read_parquet("removed_unnamed.parquet")
print("Successfully loaded data")

#removing user id, keeping only the newly added data (there may be some changes with user ids)
df_merged_data = df_merged_data.drop_duplicates(subset=['user_id'], keep='last')
print("Successfully removed duplicates")
print(df_merged_data.shape)

#save data
df_merged_data.to_parquet("removed_duplicates.parquet")
print("Successfully saved data")