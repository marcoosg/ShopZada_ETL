import pandas as pd
import numpy as np
import os
import sys

# Get the script's directory
script_directory = os.path.dirname(os.path.abspath(__file__))

# Set the working directory to the script's directory
os.chdir(script_directory)

# logs
sys.stdout = open('data_pipeline_operations_department_08_droped_products.out.log', 'w')
sys.stderr = open('data_pipeline_operations_department_08_droped_products.err.log', 'w')

df_droped_duplicates = pd.read_parquet('../data_pipeline_operations_department_07_droped_duplicates/data_pipeline_operations_department_07_droped_duplicates.parquet')
df_transactional_campaign = pd.read_parquet('../../data_pipeline_marketing_department/data_pipeline_marketing_department_03_campaign/transactional_campaign_uppercase.parquet')
print("Succesfuly Loaded Data")

# Drop Duplicate Product in an Order and keep last
df_drop_duplicate_products = df_droped_duplicates.drop_duplicates(subset=['ORDER_ID','PRODUCT_ID'], keep='last')
df_drop_duplicate_products.reset_index(drop=True, inplace=True)
print("Succesfuly Dropped Duplicate Products in an Order")

# Create Order Dimension
# Drop unnecessary columns from df_transactional_campaign
df_transactional_campaign_filtered = df_transactional_campaign.drop(['TRANSACTION_DATE', 'ESTIMATED_ARRIVAL'], axis=1)

# Merge dataframes on 'ORDER_ID'
merged_df = pd.merge(df_drop_duplicate_products, df_transactional_campaign_filtered, on='ORDER_ID', how='left')

# Aggregate data based on ORDER_ID
order_dimension = merged_df.groupby('ORDER_ID').agg({
    'PRICE': 'sum',
    'QUANTITY': 'sum',
    'DELAY_IN_DAYS': 'first',
    'TRANSACTION_DATE': lambda x: x.iloc[0] if not x.isnull().all() else None,  # Handle NaN values
    'ESTIMATED_ARRIVAL': 'first',  # Assuming ESTIMATED_ARRIVAL from df_drop_duplicate_products
    'AVAILED': 'sum'
}).reset_index()

# Create 'ORDER_CAMPAIGN_AVAILED' column as object with "YES" and "NO"
order_dimension['ORDER_CAMPAIGN_AVAILED'] = order_dimension['AVAILED'].apply(lambda x: 'YES' if x == 1 else 'NO')

# Drop unnecessary columns
order_dimension.drop(['AVAILED'], axis=1, inplace=True)

# Rename columns to match the desired format
order_dimension = order_dimension.rename(columns={
    'PRICE': 'ORDER_PRICE',
    'QUANTITY': 'ORDER_QUANTITY',
    'DELAY_IN_DAYS': 'ORDER_DELAY',
    'TRANSACTION_DATE': 'ORDER_DATE',
    'ESTIMATED_ARRIVAL': 'ORDER_EST_ARRIVAL',
})

## Saving data
order_dimension.to_parquet("data_pipeline_operations_department_08_droped_products.parquet")
print("Succesfully Saved Data")
