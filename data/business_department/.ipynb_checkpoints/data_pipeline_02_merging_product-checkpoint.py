import pandas as pd
import numpy as np

#Load data
df_product = pd.read_parquet("../business_department/product_unnamed_removed.parquet")
print("Successfully loaded product list")

#combine readymade breakfast, lunch and dinner as readymade_food
df_product.loc[df_product['product_type'] == 'readymade_breakfast', 'product_type'] = 'ready-made food'
df_product.loc[df_product['product_type'] == 'readymade_lunch', 'product_type'] = 'ready-made food'
df_product.loc[df_product['product_type'] == 'readymade_dinner', 'product_type'] = 'ready-made food'

#Change product type of bottle of paint to tools
df_product.loc[df_product['product_name'] == 'bottle of paint', 'product_type'] = 'tools'
print(df_product.shape)

#combine technology as electronics and technology
df_product.loc[df_product['product_type'] == 'technology', 'product_type'] = 'electronics and technology'

#change stationary to Stationary and School supplies
df_product.loc[df_product['product_type'] == 'stationary', 'product_type'] = 'stationary and school supplies'

#change school supplies to Stationary and School supplies
df_product.loc[df_product['product_type'] == 'school supplies', 'product_type'] = 'stationary and school supplies'

#change jewelry to accessory
df_product.loc[df_product['product_type'] == 'jewelry', 'product_type'] = 'accessories'

#change cosmetic to cosmetics
df_product.loc[df_product['product_type'] == 'cosmetic', 'product_type'] = 'cosmetics'

#change toolss to tools
df_product.loc[df_product['product_type'] == 'toolss', 'product_type'] = 'tools'

#checking product with other product type
df_product.loc[df_product['product_type'] == 'others']

#changing product type of product name balloon to toys and entertainment
df_product.loc[df_product['product_name'] == 'balloon', 'product_type'] = 'toys and entertainment'

df_product['product_type'].unique()

#Saving the data
df_product.to_parquet("product_type_merged.parquet")
print("Successfully merged the data")