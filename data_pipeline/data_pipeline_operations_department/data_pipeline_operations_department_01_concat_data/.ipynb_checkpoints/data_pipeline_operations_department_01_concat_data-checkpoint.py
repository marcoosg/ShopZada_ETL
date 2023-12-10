import pandas as pd
import numpy as np
import os
import sys

# Get the script's directory
script_directory = os.path.dirname(os.path.abspath(__file__))

# Set the working directory to the script's directory
os.chdir(script_directory)

# logs
sys.stdout = open('data_pipeline_operations_department_01_concat_data.out.log', 'w')
sys.stderr = open('data_pipeline_operations_department_01_concat_data.err.log', 'w')

# load and concat line_item_data_prices.*
df_line_item_data_prices1 = pd.read_csv(os.path.join('..', '..', '..', 'data', 'operations_department', 'line_item_data_prices1.csv'))
df_line_item_data_prices2 = pd.read_csv(os.path.join('..', '..', '..', 'data', 'operations_department', 'line_item_data_prices2.csv'))
df_line_item_data_prices3 = pd.read_parquet(os.path.join('..', '..', '..', 'data', 'operations_department', 'line_item_data_prices3.parquet'))

print("Succesfuly Loaded line_item_data_prices (3 files)")
df_line_item_data_prices_concat = pd.concat([df_line_item_data_prices1, df_line_item_data_prices2, df_line_item_data_prices3])
df_line_item_data_prices_concat.rename( columns={'Unnamed: 0':'index'}, inplace=True ) # add name to index column
df_line_item_data_prices_concat.drop(["index"], axis=1, inplace=True) #remove index columns
# Uppercase all columns
df_line_item_data_prices_concat.columns = df_line_item_data_prices_concat.columns.str.upper()
print("Succesfuly Concatinated line_item_data_prices (3 files)")

# load and concat line_item_data_products.*
df_line_item_data_products1 = pd.read_csv(os.path.join('..', '..', '..', 'data', 'operations_department', 'line_item_data_products1.csv'))
df_line_item_data_products2 = pd.read_csv(os.path.join('..', '..', '..', 'data', 'operations_department', 'line_item_data_products2.csv'))
df_line_item_data_products3 = pd.read_parquet(os.path.join('..', '..', '..', 'data', 'operations_department', 'line_item_data_products3.parquet'))

print("Succesfuly Loaded line_item_data_products (3 files)")
df_line_item_data_products_concat = pd.concat([df_line_item_data_products1, df_line_item_data_products2, df_line_item_data_products3])
df_line_item_data_products_concat.rename( columns={'Unnamed: 0':'index'}, inplace=True ) # add name to index column
df_line_item_data_products_concat.drop(["index"], axis=1, inplace=True) #remove index columns
df_line_item_data_products_concat.columns = df_line_item_data_products_concat.columns.str.upper()
print("Succesfuly Concatinated line_item_data_products (3 files)")

# load and concat order_data_.*
df_order_data1 = pd.read_parquet(os.path.join('..', '..', '..', 'data', 'operations_department', 'order_data_20200101-20200701.parquet'))
df_order_data2 = pd.read_pickle(os.path.join('..', '..', '..', 'data', 'operations_department', 'order_data_20200701-20211001.pickle'))
df_order_data3 = pd.read_csv(os.path.join('..', '..', '..', 'data', 'operations_department', 'order_data_20211001-20220101.csv'))
df_order_data4 = pd.read_excel(os.path.join('..', '..', '..', 'data', 'operations_department', 'order_data_20220101-20221201.xlsx'))
df_order_data5 = pd.read_json(os.path.join('..', '..', '..', 'data', 'operations_department', 'order_data_20221201-20230601.json'))
df_order_data6_list = pd.read_html(os.path.join('..', '..', '..', 'data', 'operations_department', 'order_data_20230601-20240101.html'))
df_order_data6 = df_order_data6_list[0]
print("Succesfuly Loaded order_data (6 files)")
df_order_data_concat = pd.concat([df_order_data1, df_order_data2, df_order_data3, df_order_data4, df_order_data5, df_order_data6])
df_order_data_concat.rename( columns={'Unnamed: 0':'index'}, inplace=True ) # add name to index column
df_order_data_concat.drop(["index"], axis=1, inplace=True) #remove index columns
df_order_data_concat.reset_index(drop=True, inplace=True) # reset index
df_order_data_concat.columns = df_order_data_concat.columns.str.upper()
print("Succesfuly Concatinated order_data (6 files)")

# load order_delays.html
df_order_delays_list = pd.read_html(os.path.join('..', '..', '..', 'data', 'operations_department', 'order_delays.html'))
df_order_delays = df_order_delays_list[0]
df_order_delays.rename( columns={'Unnamed: 0':'index'}, inplace=True ) # add name to index column
df_order_delays.drop(["index"], axis=1, inplace=True) #remove index
df_order_delays.columns = df_order_delays.columns.str.upper()
print("Succesfuly Loaded order_delays")


## Saving data
df_line_item_data_prices_concat.to_parquet("line_item_data_prices_concat.parquet")
df_line_item_data_products_concat.to_parquet("line_item_data_products_concat.parquet")
df_order_data_concat.to_parquet("order_data_concat.parquet")
df_order_delays.to_parquet("order_delays.parquet")
print("Succesfully Saved Data")



