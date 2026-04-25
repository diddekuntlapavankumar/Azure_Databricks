# Databricks notebook source
# ETL Trains Pipeline
#
# Author: Pavan Kumar Reddy Duddekuntla
# Role: Senior Data Engineer
#
# Overview:
#   Config-driven medallion architecture pipeline for Indian Railways
#   train data (EXP-TRAINS.json). Reads from ADLS Gen2, processes
#   through Bronze → Silver → Gold layers using Delta Lake.
#
# Configuration:
#   Config file : Azure_Databricks/etl_config_trains.json
#   Source      : ainput1/EXP-TRAINS.json (ADLS Gen2)
#   Layers      : Bronze (raw flattened) → Silver (cleansed) → Gold (aggregated)
#
# Modules:
#   Bronze_Layer_Trains.py  - Raw JSON ingestion and flattening
#   Silver_Layer_Trains.py  - Data cleansing, type casting, quarantine
#   Gold_Layer_Trains.py    - Route summary, station frequency, running days


# ======================================================================
# SECTION 1: Import ADLSManager and Initialize Config
# ======================================================================

import sys
import os

# Add the project directory to Python path
sys.path.insert(0, "/Workspace/Users/pavanreddy_adf@outlook.com/Azure_Databricks")

# Import the ADLSManager class from ADLS_Databricks_Connection module
from ADLS_Databricks_Connection import ADLSManager

# Initialize ADLSManager with the trains ETL config
config_path = "/Workspace/Users/pavanreddy_adf@outlook.com/Azure_Databricks/etl_config_trains.json"
manager = ADLSManager(config_path)

# Print configuration summary
manager.print_summary()


# ======================================================================
# SECTION 2: Configure OAuth for ADLS Gen2
# ======================================================================

import ADLS_Databricks_Connection as adls_module
adls_module.dbutils = dbutils
adls_module.spark = spark

# Configure OAuth for ADLS Gen2 access
manager.configure_oauth()


# ======================================================================
# SECTION 3: Bronze Layer (via Bronze_Layer_Trains module)
# ======================================================================

from Bronze_Layer_Trains import BronzeLayerTrains

bronze = BronzeLayerTrains(manager, spark)
df_bronze = bronze.run()

print(f"\nBronze output: {df_bronze.count():,} rows | {len(df_bronze.columns)} columns\n")


# ======================================================================
# SECTION 4: Silver Layer (via Silver_Layer_Trains module)
# ======================================================================

from Silver_Layer_Trains import SilverLayerTrains

silver = SilverLayerTrains(manager, spark)
df_silver_clean = silver.run()

print(f"\nSilver output: {df_silver_clean.count():,} rows | {len(df_silver_clean.columns)} columns\n")


# ======================================================================
# SECTION 5: Gold Layer (via Gold_Layer_Trains module)
# ======================================================================

from Gold_Layer_Trains import GoldLayerTrains

gold = GoldLayerTrains(manager, spark)
gold_tables = gold.run()

print(f"\nGold tables produced: {list(gold_tables.keys())}\n")


# ======================================================================
# PIPELINE COMPLETE
# ======================================================================
bronze_path = manager.get_path(manager.config["medallion_paths"]["bronze"])
silver_path = manager.get_path(manager.config["medallion_paths"]["silver"])
gold_route_path = manager.get_path(manager.config["medallion_paths"]["gold_route_summary"])
gold_station_path = manager.get_path(manager.config["medallion_paths"]["gold_station_frequency"])
gold_days_path = manager.get_path(manager.config["medallion_paths"]["gold_running_days"])

print("\n" + "=" * 55)
print("  ETL Trains Pipeline - Complete")
print("=" * 55)
print(f"  Source             : EXP-TRAINS.json")
print(f"  Bronze             : {bronze_path}")
print(f"  Silver             : {silver_path}")
print(f"  Gold Route Summary : {gold_route_path}")
print(f"  Gold Station Freq  : {gold_station_path}")
print(f"  Gold Running Days  : {gold_days_path}")
print("=" * 55)
