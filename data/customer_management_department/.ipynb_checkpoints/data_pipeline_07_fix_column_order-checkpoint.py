import pandas as pd
import numpy as np

#load data
df_merged_data=pd.read_parquet("fixed_column_name.parquet")
print("Successfully loaded data")

print(df_merged_data.iloc[3000:4000])

#fix column order
df_merged_data = df_merged_data[['USER_ID', 'USER_CREATION_DATE', 'USER_NAME', 'USER_ADDR_STREET', 'USER_ADDR_STATE', 'USER_ADDR_CITY', 'USER_ADDR_COUNTRY', 'USER_BIRTHDATE','USER_GENDER', 'USER_DEVICE_ADDR', 'USER_TYPE', 'USER_JOB_TITLE', 'USER_JOB_LVL' ,'USER_CC_NO', 'USER_ISSUING_BANK']]
print("Successfully fixed column order")
print(df_merged_data.iloc[3000:4000])

#save data
df_merged_data.to_parquet("column_order.parquet")

