import pandas as pd
import numpy as np
import os
import sys

# Get the script's directory
script_directory = os.path.dirname(os.path.abspath(__file__))

# Set the working directory to the script's directory
os.chdir(script_directory)

# logs
sys.stdout = open('data_pipeline_operations_department_01_concat_data.out.log', 'w')
sys.stderr = open('data_pipeline_operations_department_01_concat_data.err.log', 'w')

#load data
df_merged_data=pd.read_parquet('../data_pipeline_03_remove_duplicates/removed_duplicates.parquet')
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