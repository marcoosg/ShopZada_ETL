import pickle
import pandas as pd
import numpy as np
import os
import sys

# Get the script's directory
script_directory = os.path.dirname(os.path.abspath(__file__))

# Set the working directory to the script's directory
os.chdir(script_directory)

# logs
sys.stdout = open('data_pipeline_01_fix_job_title.out.log', 'w')
sys.stderr = open('data_pipeline_01_fix_job_title.err.log', 'w')


#load data
with open('../../../data/customer_management_department/user_credit_card.pickle', 'rb') as user_cc:
    df_user_credit_card = pickle.load(user_cc)
df_user_data = pd.read_json("../../../data/customer_management_department/user_data.json")
df_user_job = pd.read_csv('../../../data/customer_management_department/user_job.csv')
df_merged_data = df_user_credit_card.merge(df_user_data)
df_merged_data = df_merged_data.merge(df_user_job)
print("loaded data successfully")
print(df_merged_data.info())

# changing student job level to N/A
df_merged_data.loc[df_merged_data['job_title'] == 'Student', 'job_level'] = 'N/A'

#Saved data
df_merged_data.to_parquet("fixed_job_title.parquet")
print("Successfully saved data")