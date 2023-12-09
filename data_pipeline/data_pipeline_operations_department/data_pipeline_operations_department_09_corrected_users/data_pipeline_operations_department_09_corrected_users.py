import pandas as pd
import numpy as np
import os
import sys

# Get the script's directory
script_directory = os.path.dirname(os.path.abspath(__file__))

# Set the working directory to the script's directory
os.chdir(script_directory)

# logs
sys.stdout = open('data_pipeline_operations_department_09_corrected_users.out.log', 'w')
sys.stderr = open('data_pipeline_operations_department_09_corrected_users.err.log', 'w')

# Load data
df_drop_duplicate_products = pd.read_parquet('../data_pipeline_operations_department_08_droped_products/data_pipeline_operations_department_08_droped_products.parquet')
df_user_list = pd.read_parquet('../../data_pipeline_customer_management_department/data_pipeline_07_fix_column_order/column_order.parquet')

df_transactional_campaign = pd.read_parquet('../../data_pipeline_marketing_department/data_pipeline_marketing_department_03_campaign/transactional_campaign_uppercase.parquet')
print("Succesfuly Loaded Data")

# Drop rows in df_drop_duplicate_products where USER_ID does not exist in df_user_list
df_corrected_users = df_drop_duplicate_products.copy()
df_corrected_users.drop(df_drop_duplicate_products[~df_drop_duplicate_products['USER_ID'].isin(df_user_list['USER_ID'])].index, inplace=True)
print("Succesfuly Dropped rows with Non-Existent User (if any)")



# Separating cleaned tables
df_order_data = df_corrected_users[['ORDER_ID', 'USER_ID', 'ESTIMATED_ARRIVAL', 'TRANSACTION_DATE']].copy()
df_order_delays = df_corrected_users[['ORDER_ID', 'DELAY_IN_DAYS']].copy()
df_line_item_data_prices = df_corrected_users[['ORDER_ID', 'PRICE', 'QUANTITY']].copy()
df_line_item_data_products = df_corrected_users[['ORDER_ID', 'PRODUCT_NAME', 'PRODUCT_ID']].copy()

df_order_data = df_order_data.drop_duplicates(subset=['ORDER_ID'], keep='first')
df_order_data.reset_index(drop=True, inplace=True)

df_order_delays = df_order_delays.drop_duplicates(subset=['ORDER_ID'], keep='first')
# Delete rows where 'DELAY_IN_DAYS' is 0
df_order_delays = df_order_delays[df_order_delays['DELAY_IN_DAYS'] != 0]
df_order_delays.reset_index(drop=True, inplace=True)



# Saving cleaned tables
df_corrected_users.to_parquet("order_data_merged.parquet")
df_order_data.to_parquet("order_data.parquet")
df_order_delays.to_parquet("order_delays.parquet")
df_line_item_data_prices.to_parquet("line_item_data_prices.parquet")
df_line_item_data_products.to_parquet("line_item_data_products.parquet")
print("Succesfully Saved order_data")





# Create Order Dimension
# Drop unnecessary columns from df_transactional_campaign
df_transactional_campaign_filtered = df_transactional_campaign.drop(['TRANSACTION_DATE', 'ESTIMATED_ARRIVAL'], axis=1)

# Merge dataframes on 'ORDER_ID'
merged_df = pd.merge(df_corrected_users, df_transactional_campaign_filtered, on='ORDER_ID', how='left')

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
print("Succesfuly Created orderDimension")

## Saving data
order_dimension.to_parquet("order_dimension.parquet")
print("Succesfully Saved orderDimension")


