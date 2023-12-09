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
sys.stdout = open('data_pipeline_enterprise_department_merchant_data_04_uppercase_data.out.log', 'w')
sys.stderr = open('data_pipeline_enterprise_department_merchant_data_04_uppercase_data.err.log', 'w')

#load data
df_merchant = pd.read_parquet('../data_pipeline_enterprise_department_merchant_data_03_contact_number_to_int/contact_number_to_int.parquet')
print("Successfully loaded data")

#uppercasing data
df_merchant.loc[:, 'name'] = df_merchant['name'].str.upper()
df_merchant.loc[:, 'street'] = df_merchant['street'].str.upper()
df_merchant.loc[:, 'state'] = df_merchant['state'].str.upper()
df_merchant.loc[:, 'city'] = df_merchant['city'].str.upper()
df_merchant.loc[:, 'country'] = df_merchant['country'].str.upper()
print("Successfully uppercased data")

#uppercasing columns
df_merchant.columns = df_merchant.columns.str.upper()
print("Successfully uppercased columns")

#save data
df_merchant.to_parquet("fixed_merchant_data.parquet")
print("Successfully saved data")