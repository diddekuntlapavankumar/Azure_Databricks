# Databricks notebook source
# Read Taxi Trip Data from ADLS Gen2
#
# Author: Pavan Kumar Reddy Duddekuntla
# Role: Senior Data Engineer
#
# Overview:
#   Reads taxi_tripdata.csv from Azure Data Lake Storage Gen2
#   using the ADLSManager class from ADLS_Databricks_Connection.py.
#
# Pipeline steps:
#   1. Load the ADLS connectivity module
#   2. Initialize ADLSManager with etl_config.json
#   3. Configure OAuth authentication
#   4. Read taxi_tripdata.csv from ADLS
#   5. Explore and display the data


import os
import sys

# ======================================================================
# SECTION 1: Load ADLS Connection Module
# ======================================================================

module_path = "/Workspace/Users/pavanreddy_adf@outlook.com/Azure_Databricks/ADLS_Databricks_Connection.py"

with open(module_path, "r") as f:
    exec(f.read())

print("Module loaded successfully.")

# ======================================================================
# SECTION 2: Initialize ADLSManager and Configure OAuth
# ======================================================================

config_path = "/Workspace/Users/pavanreddy_adf@outlook.com/Azure_Databricks/etl_config.json"

adls = ADLSManager(config_path)
adls.configure_oauth()
adls.print_summary()

# ======================================================================
# SECTION 3: Read taxi_tripdata.csv from ADLS
# ======================================================================

csv_path = adls.file_cfg["csv"]  # ainput1/taxi_tripdata.csv

df_taxi = adls.read_file(csv_path, file_format="csv", header="true", inferSchema="true")
print(f"Rows: {df_taxi.count():,}  |  Columns: {len(df_taxi.columns)}")

# ======================================================================
# SECTION 4: Display Schema and Preview
# ======================================================================

df_taxi.printSchema()
display(df_taxi)

# ======================================================================
# SECTION 5: Summary Statistics
# ======================================================================

display(df_taxi.summary())

print("\n✅ Taxi tripdata pipeline completed successfully!")
