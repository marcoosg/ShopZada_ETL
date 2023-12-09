import pandas as pd
import numpy as np

#Load data
df_product = pd.read_excel("../business_department/product_list.xlsx")
print("Successfully loaded product list")

#Sorting
print("Data frame size before removing unnamed: ")
print(df_product.shape)
df_product = df_product.loc[:, ~df_product.columns.str.contains('^Unnamed')]
print("Unnamed columns successfully removed")
print("Data frame size after removing unnamed: ")
print(df_product.shape)

#save data
df_product.to_parquet("product_unnamed_removed.parquet")
print("Successfully saved data")