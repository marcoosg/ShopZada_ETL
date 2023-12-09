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
sys.stdout = open('data_pipeline_enterprise_department_merchant_data_01_convert_to_date.out.log', 'w')
sys.stderr = open('data_pipeline_enterprise_department_merchant_data_01_convert_to_date.err.log', 'w')

#load data
df_merchant_list = pd.read_html('../../../data/enterprise_department/merchant_data.html')
print("Successfully loaded data")
df_merchant = df_merchant_list[0]

#convert to date
df_merchant.loc[:,'creation_date'] = pd.to_datetime(df_merchant['creation_date']).dt.date
print(df_merchant.head())

#save data
df_merchant.to_parquet("convert_to_date.parquet")

