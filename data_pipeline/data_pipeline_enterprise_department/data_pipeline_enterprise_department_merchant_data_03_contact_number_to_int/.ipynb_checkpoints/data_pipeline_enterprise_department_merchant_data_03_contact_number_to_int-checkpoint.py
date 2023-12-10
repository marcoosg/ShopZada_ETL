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
sys.stdout = open('data_pipeline_enterprise_department_merchant_data_03_contact_number_to_int.out.log', 'w')
sys.stderr = open('data_pipeline_enterprise_department_merchant_data_03_contact_number_to_int.err.log', 'w')

#load data
df_merchant = pd.read_parquet(os.path.join('..', 'data_pipeline_enterprise_department_merchant_data_02_remove_duplicates', 'duplicates_removed.parquet'))
print("Successfully loaded data")

#Contact number string to int
df_merchant.loc[:, 'contact_number'] = df_merchant['contact_number'].str.replace(r'[^0-9]', '', regex=True)
print("Contact number successfully converted to int")

#save data
df_merchant.to_parquet("contact_number_to_int.parquet")
print("Successfully saved data")