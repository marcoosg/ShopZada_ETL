import pandas as pd
import numpy as np

#Load data
df_product = pd.read_parquet("../business_department/product_duplicates_removed.parquet")
print("Successfully loaded the data")

df_product[df_product.duplicated(['product_name'], keep=False)]

#change product name bow tie to accessory
df_product.loc[df_product['product_name'] == 'bow tie', 'product_type'] = 'accessories'

#change product name washing machine to appliances
df_product.loc[df_product['product_name'] == 'washing machine', 'product_type'] = 'appliances'

#change product name mouse pad to accessories
df_product.loc[df_product['product_name'] == 'mouse pad', 'product_type'] = 'accessories'

#removing same product name and product type, keeping only the newly added data
df_product = df_product.drop_duplicates(subset=["product_name","product_type"], keep="last")
print(df_product.nunique())

#Save data
df_product.to_parquet("product_type_changes.parquet")
print("Successfully saved the data")