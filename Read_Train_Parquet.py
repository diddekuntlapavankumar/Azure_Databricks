# Databricks notebook source
# Read Train Parquet from ADLS Gen2
#
# Author: Pavan Kumar Reddy Duddekuntla
# Role: Senior Data Engineer
#
# Overview:
#   Reads train.parquet from Azure Data Lake Storage Gen2 using the
#   ADLSManager class from ADLS_Databricks_Connection.py.
#   Displays schema, row counts, and summary statistics.
#
# Configuration:
#   Config file     : Azure_Databricks/etl_config_trains.json
#   Secret scope    : adls-scope
#   Storage account : bronzeadlsstorage
#   Container       : ainput


# ======================================================================
# SECTION 1: Import ADLS Connection Module
# ======================================================================

import sys
import os

# Add the project directory to Python path
sys.path.insert(0, "/Workspace/Users/pavanreddy_adf@outlook.com/Azure_Databricks")

# Import the ADLSManager class
from ADLS_Databricks_Connection import ADLSManager


# ======================================================================
# SECTION 2: Initialize ADLSManager and Configure OAuth
# ======================================================================

import ADLS_Databricks_Connection as adls_module
adls_module.dbutils = dbutils
adls_module.spark = spark

# Initialize ADLSManager with the trains ETL config
config_path = "/Workspace/Users/pavanreddy_adf@outlook.com/Azure_Databricks/etl_config_trains.json"

manager = ADLSManager(config_path)
manager.configure_oauth()
manager.print_summary()


# ======================================================================
# SECTION 3: Read train.parquet from ADLS
# ======================================================================

# Read train.parquet using ADLSManager
df_train = manager.read_file(
    file_path="ainput1/train.parquet",
    file_format="parquet"
)

print(f"Rows: {df_train.count():,}")
print(f"Columns: {len(df_train.columns)}")
df_train.printSchema()


# ======================================================================
# SECTION 4: Display the Data
# ======================================================================

display(df_train)


# ======================================================================
# SECTION 5: Basic Summary Statistics
# ======================================================================

# Summary statistics
display(df_train.describe())
