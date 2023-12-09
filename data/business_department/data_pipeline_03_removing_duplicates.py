import pandas as pd
import numpy as np

#Load data
df_product = pd.read_parquet("../business_department/product_type_merged.parquet")
print("Successfully loaded the data")

#removing same product name and product type, keeping only the newly added data
df_product = df_product.drop_duplicates(subset=["product_name","product_type"], keep="last")
print(df_product.shape)

#removing product id, keeping only the newly added data (there may be some changes with product ids)
df_product = df_product.drop_duplicates(subset=['product_id'], keep='last')
print(df_product.shape)
print(df_product.nunique())

#Saving the data
df_product.to_parquet("product_duplicates_removed.parquet")
print("Successfully removed duplicates")