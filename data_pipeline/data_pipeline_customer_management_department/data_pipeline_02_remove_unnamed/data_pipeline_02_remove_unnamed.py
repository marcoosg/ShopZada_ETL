import pandas as pd
import numpy as np
import os
import sys

# Get the script's directory
script_directory = os.path.dirname(os.path.abspath(__file__))

# Set the working directory to the script's directory
os.chdir(script_directory)

# logs
sys.stdout = open('data_pipeline_02_remove_unnamed.out.log', 'w')
sys.stderr = open('data_pipeline_02_remove_unnamed.err.log', 'w')

#load data
df_merged_data=pd.read_parquet('../data_pipeline_01_fix_job_title/fixed_job_title.parquet')
print("Successfully loaded data")

#removing unnamed 
df_merged_data.drop('Unnamed: 0', inplace=True, axis=1)
print ("Unnamed removed")
print(df_merged_data.shape)

#save data
df_merged_data.to_parquet("removed_unnamed.parquet")
print("Successfully saved data")