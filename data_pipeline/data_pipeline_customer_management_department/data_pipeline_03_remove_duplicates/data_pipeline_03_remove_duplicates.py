import pandas as pd
import numpy as np
import os
import sys

# Get the script's directory
script_directory = os.path.dirname(os.path.abspath(__file__))

# Set the working directory to the script's directory
os.chdir(script_directory)

# logs
sys.stdout = open('data_pipeline_03_remove_duplicates.out.log', 'w')
sys.stderr = open('data_pipeline_03_remove_duplicates.err.log', 'w')

#load data
df_merged_data=pd.read_parquet('../data_pipeline_02_remove_unnamed/removed_unnamed.parquet')
print("Successfully loaded data")

#removing user id, keeping only the newly added data (there may be some changes with user ids)
df_merged_data = df_merged_data.drop_duplicates(subset=['user_id'], keep='last')
print("Successfully removed duplicates")
print(df_merged_data.shape)

#save data
df_merged_data.to_parquet("removed_duplicates.parquet")
print("Successfully saved data")