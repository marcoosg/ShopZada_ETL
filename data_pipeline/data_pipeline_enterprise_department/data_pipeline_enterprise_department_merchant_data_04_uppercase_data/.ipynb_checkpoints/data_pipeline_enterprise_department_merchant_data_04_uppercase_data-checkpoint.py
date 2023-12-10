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
df_merchant = pd.read_parquet(os.path.join('..', 'data_pipeline_enterprise_department_merchant_data_03_contact_number_to_int', 'contact_number_to_int.parquet'))
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
df_merchant.to_parquet("merchant_data.parquet")

# Rename columns in df_merchant
df_merchant.rename(columns={
    'MERCHANT_ID': 'MERCHANT_ID',
    'CREATION_DATE': 'MERCHANT_CREATION_DATE',
    'NAME': 'MERCHANT_NAME',
    'STREET': 'MERCHANT_STREET',
    'STATE': 'MERCHANT_STATE',
    'CITY': 'MERCHANT_CITY',
    'COUNTRY': 'MERCHANT_COUNTRY',
    'CONTACT_NUMBER': 'MERCHANT_CONTACT_NO'
}, inplace=True)
print("Successfully created merchantDimension")

#save data
df_merchant.to_parquet("merchant_dimension.parquet")
print("Successfully saved data")