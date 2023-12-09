import pandas as pd
import numpy as np
import lxml
import html5lib
import phonenumbers as pn
import os
import sys

# Get the script's directory
script_directory = os.path.dirname(os.path.abspath(__file__))

# Set the working directory to the script's directory
os.chdir(script_directory)

# logs
sys.stdout = open('data_pipeline_enterprise_department_staff_data_01_remove_duplicates.out.log', 'w')
sys.stderr = open('data_pipeline_enterprise_department_staff_data_01_remove_duplicates.err.log', 'w')

#load data
df_staff_list = pd.read_html('../../../data/enterprise_department/staff_data.html')
print("Successfully loaded data")

#sorting according to date
df_staff = df_staff_list[0]
df_staff.loc[:,'creation_date'] = pd.to_datetime(df_staff['creation_date']).dt.date
df_staff = df_staff.sort_values(by='creation_date')
print("Successfully sorted data")

#remove duplicates
print("Number of records before removing duplicates:")
print(df_staff.shape)
df_staff.drop_duplicates()
df_staff = df_staff.drop_duplicates(subset=['staff_id'],keep='last')
print("Successfully removed duplicates")
print("Number of records after removing duplicates:")
print(df_staff.shape)

#remove columns with 0 data
del df_staff['Unnamed: 0']

#save data
df_staff.to_parquet("removed_duplicates.parquet")
print("Successfully saved data")
