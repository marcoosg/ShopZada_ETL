import pandas as pd
import numpy as np
import os
import sys

# Get the script's directory
script_directory = os.path.dirname(os.path.abspath(__file__))

# Set the working directory to the script's directory
os.chdir(script_directory)

# logs
sys.stdout = open('data_pipeline_load_to_database_01_load_dimensions.out.log', 'w')
sys.stderr = open('data_pipeline_load_to_database_01_load_dimensions.err.log', 'w')

# Load data
df_order_dimension = pd.read_parquet('')
df_product_dimension = pd.read_parquet('')
df_user_dimension = pd.read_parquet('')
df_staff_dimension = pd.read_parquet('')
df_merchant_dimension = pd.read_parquet('')
df_campaign_dimension = pd.read_parquet('')