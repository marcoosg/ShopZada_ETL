import pandas as pd
import numpy as np
import sqlite3
import os
import sys

# Get the script's directory
script_directory = os.path.dirname(os.path.abspath(__file__))

# Set the working directory to the script's directory
os.chdir(script_directory)

# logs
sys.stdout = open('data_pipeline_load_to_database_01_load_dimensions.out.log', 'w')
sys.stderr = open('data_pipeline_load_to_database_01_load_dimensions.err.log', 'w')

# Load tables from parquet for Fact Table
# Operations Department
df_order_data_merged = pd.read_parquet(os.path.join('..', '..', 'data_pipeline_operations_department', 'data_pipeline_operations_department_09_corrected_users', 'order_data_merged.parquet'))

# Marketing Department
df_transactional_campaign_data = pd.read_parquet(os.path.join('..', '..', 'data_pipeline_marketing_department', 'data_pipeline_marketing_department_03_campaign', 'transactional_campaign_uppercase.parquet'))

# Enterprise Department
df_order_with_merchant_data = pd.read_parquet(os.path.join('..', '..', 'data_pipeline_enterprise_department', 'data_pipeline_enterprise_department_order_with_merchant_01_uppercase_columns', 'fixed_order_with_merchant.parquet'))
print("Successfully Loaded Data for Fact Table")

# Load Dimension tables
df_order_dimension = pd.read_parquet(os.path.join('..', '..', 'data_pipeline_operations_department', 'data_pipeline_operations_department_09_corrected_users', 'order_dimension.parquet'))

df_product_dimension = pd.read_parquet(os.path.join('..', '..', 'data_pipeline_business_department', 'data_pipeline_05_structuring_columns', 'product_dimension.parquet'))

df_user_dimension = pd.read_parquet(os.path.join('..', '..', 'data_pipeline_customer_management_department', 'data_pipeline_07_fix_column_order', 'column_order.parquet'))

df_staff_dimension = pd.read_parquet(os.path.join('..', '..', 'data_pipeline_enterprise_department', 'data_pipeline_enterprise_department_staff_data_03_uppercase_data', 'staff_dimension.parquet'))

df_merchant_dimension = pd.read_parquet(os.path.join('..', '..', 'data_pipeline_enterprise_department', 'data_pipeline_enterprise_department_merchant_data_04_uppercase_data', 'merchant_dimension.parquet'))

df_campaign_dimension = pd.read_parquet(os.path.join('..', '..', 'data_pipeline_marketing_department', 'data_pipeline_marketing_department_02_campaign', 'campaign_dimension.parquet'))

print("Successfully Loaded Dimensions")

# Fact Table
# ORDER_ID* = order_data
# USER_ID* = order_data
# PRODUCT_ID* = line_item_data_products
# PRODUCT_QUANTITY = line_item_data_products
# STAFF_ID*
# MERCHANT_ID*
# CAMPAIGN_ID*

# Merge order_data, line_item_data_prices, user, order_with_merchant_data, and transactional_campaign_data
df_fact_table_01 = df_order_data_merged.copy()

df_fact_table_02 = df_fact_table_01[['ORDER_ID', 'USER_ID', 'PRODUCT_ID', 'QUANTITY']]

df_fact_table_03 = pd.merge(df_fact_table_02, df_order_with_merchant_data, on='ORDER_ID', how='left')

df_fact_table_04 = pd.merge(df_fact_table_03, df_transactional_campaign_data, on='ORDER_ID', how='left')

df_fact_table_05 = df_fact_table_04[['ORDER_ID', 'USER_ID', 'PRODUCT_ID', 'QUANTITY', 'STAFF_ID', 'MERCHANT_ID', 'CAMPAIGN_ID']]
df_fact_table_05 = df_fact_table_05.rename(columns={'QUANTITY': 'PRODUCT_QUANTITY'})

df_fact_table = df_fact_table_05.copy()

# Load Dimensions
db_connection = sqlite3.connect(os.path.join('..', '..','..', 'database','shopzada.db'))
cursor = db_connection.cursor()
df_order_dimension.to_sql('order_dimension', db_connection, index=False, if_exists='replace')
df_product_dimension.to_sql('product_dimension', db_connection, index=False, if_exists='replace')
df_user_dimension.to_sql('user_dimension', db_connection, index=False, if_exists='replace')
df_staff_dimension.to_sql('staff_dimension', db_connection, index=False, if_exists='replace')
df_merchant_dimension.to_sql('merchant_dimension', db_connection, index=False, if_exists='replace')
df_campaign_dimension.to_sql('campaign_dimension', db_connection, index=False, if_exists='replace')
df_fact_table.to_sql('shopzada_fact', db_connection, index=False, if_exists='replace')

