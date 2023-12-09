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
sys.stdout = open('data_pipeline_enterprise_department_staff_data_03_uppercase_data.out.log', 'w')
sys.stderr = open('data_pipeline_enterprise_department_staff_data_03_uppercase_data.err.log', 'w')

#load data
df_staff = pd.read_parquet('../data_pipeline_enterprise_department_staff_data_02_contact_number_to_int/contact_number_to_int.parquet')
print("Successfully loaded data")

#uppercase data
df_staff.loc[:, 'name'] = df_staff['name'].str.upper()
df_staff.loc[:, 'job_level'] = df_staff['job_level'].str.upper()
df_staff.loc[:, 'street'] = df_staff['street'].str.upper()
df_staff.loc[:, 'state'] = df_staff['state'].str.upper()
df_staff.loc[:, 'city'] = df_staff['city'].str.upper()
df_staff.loc[:, 'country'] = df_staff['country'].str.upper()
print("Successfully uppercased data")

#uppercase columns
df_staff.columns = df_staff.columns.str.upper()
print("Successfully uppercased columns")

#save data
df_staff.to_parquet("uppercased_data.parquet")
print("Successfully saved data")