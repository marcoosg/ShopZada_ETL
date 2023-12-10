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
sys.stdout = open('data_pipeline_enterprise_department_staff_data_02_contact_number_to_int.out.log', 'w')
sys.stderr = open('data_pipeline_enterprise_department_staff_data_02_contact_number_to_int.err.log', 'w')

#load data
df_staff = pd.read_parquet(os.path.join('..', 'data_pipeline_enterprise_department_staff_data_01_remove_duplicates', 'removed_duplicates.parquet'))
print("Successfully loaded data")

#convert contact number to int
df_staff.loc[:, 'contact_number'] = df_staff['contact_number'].str.replace(r'[^0-9]', '', regex=True)
print("Successfully converted contact number to int")

#save data
df_staff.to_parquet("contact_number_to_int.parquet")
print("Successfully saved data")