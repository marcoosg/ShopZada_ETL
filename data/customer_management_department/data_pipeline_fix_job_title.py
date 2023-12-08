import pickle
import pandas as pd
import numpy as np

#load data
with open('user_credit_card.pickle', 'rb') as user_cc:
    df_user_credit_card = pickle.load(user_cc)
df_user_data = pd.read_json("user_data.json")
df_user_job = pd.read_csv('user_job.csv')
df_merged_data = df_user_credit_card.merge(df_user_data)
df_merged_data = df_merged_data.merge(df_user_job)
print("loaded data successfully")
print(df_merged_data.info())

# changing student job level to N/A
df_merged_data.loc[df_merged_data['job_title'] == 'Student', 'job_level'] = 'N/A'

#Saved data
df_merged_data.to_parquet("fixed_job_title.parquet")
print("Successfully saved data")