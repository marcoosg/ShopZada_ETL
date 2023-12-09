import pandas as pd
import numpy as np

#Load data
df_product = pd.read_parquet("../business_department/product_type_changes.parquet")
print("Successfully loaded the data")

#renaming columns
df_product = df_product.rename(columns={'price':'product_price'})
df_product['product_type'] = df_product['product_type'].str.upper()
df_product['product_name'] = df_product['product_name'].str.upper()
# Uppercase all columns
df_product.columns = df_product.columns.str.upper()
print(df_product.head(30))

#Saving data
df_product.to_parquet("product_column_structure.parquet")
df_product.to_parquet("product_final_list.parquet")
print("Successfully saved data")