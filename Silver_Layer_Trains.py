# Databricks notebook source
# Silver Layer - Trains ETL
#
# Author: Pavan Kumar Reddy Duddekuntla
# Role: Senior Data Engineer
#
# Overview:
#   Silver layer module for the Indian Railways train data pipeline.
#   Reads Bronze Delta, applies data cleansing and type transformations,
#   quarantines bad records, and writes clean data to Silver Delta.
#
# Input  : medallion/bronze/exp_trains (Delta - raw flattened stops)
# Output : medallion/silver/exp_trains (Delta - cleansed stops)
#          medallion/bronze/exp_trains_quarantine (Delta - rejected rows)


from pyspark.sql.functions import (
    col, trim, regexp_extract, when, lit, current_timestamp
)
from pyspark.sql.types import IntegerType, DoubleType


class SilverLayerTrains:
    """Handles Silver layer cleansing: type casts, parsing, quarantine, and persist."""

    def __init__(self, manager, spark):
        """Initialize with ADLSManager instance and Spark session."""
        self.manager = manager
        self.spark = spark
        self.df_bronze = None
        self.df_silver = None
        self.df_silver_clean = None
        self.df_quarantine = None
        self.bronze_path = manager.get_path(
            manager.config["medallion_paths"]["bronze"]
        )
        self.silver_path = manager.get_path(
            manager.config["medallion_paths"]["silver"]
        )
        self.quarantine_path = manager.get_path(
            manager.config["medallion_paths"]["quarantine"]
        )

    # ------------------------------------------------------------------ #
    #  Step 1: Read Bronze Delta layer                                     #
    # ------------------------------------------------------------------ #

    def read_bronze(self):
        """Read the Bronze Delta table from ADLS."""
        self.df_bronze = self.spark.read.format("delta").load(self.bronze_path)
        row_count = self.df_bronze.count()
        print(f"✔ Bronze loaded: {row_count:,} rows")
        return self

    # ------------------------------------------------------------------ #
    #  Step 2: Cleanse and transform                                       #
    # ------------------------------------------------------------------ #

    def cleanse_and_transform(self):
        """Apply all data cleansing and transformation rules."""
        if self.df_bronze is None:
            raise RuntimeError("Call read_bronze() before cleanse_and_transform()")

        self.df_silver = (
            self.df_bronze
            # 1. Trim whitespace on string columns
            .withColumn("trainNumber", trim(col("trainNumber")))
            .withColumn("trainName",   trim(col("trainName")))
            .withColumn("route",       trim(col("route")))
            .withColumn("station_name", trim(col("station_name")))

            # 2. Split station_name "MARWAR JN - MJ" -> name + code
            .withColumn("station_code",
                trim(regexp_extract(col("station_name"), r"- (.+)$", 1)))
            .withColumn("station_name_clean",
                trim(regexp_extract(col("station_name"), r"^(.+?) -", 1)))

            # 3. Split route into origin and destination
            .withColumn("origin",
                trim(regexp_extract(col("route"), r"^(.+?) to ", 1)))
            .withColumn("destination",
                trim(regexp_extract(col("route"), r" to (.+)$", 1)))

            # 4. Cast stop_number and day to integer
            .withColumn("stop_number", col("stop_number").cast(IntegerType()))
            .withColumn("day",         col("day").cast(IntegerType()))

            # 5. Parse distance: "25 kms" -> 25.0
            .withColumn("distance_km",
                regexp_extract(col("distance"), r"(\d+)", 1).cast(DoubleType()))

            # 6. Flag source/destination stops and normalize arrival/departure
            .withColumn("is_source",      col("arrives") == "Source")
            .withColumn("is_destination", col("departs") == "Destination")
            .withColumn("arrival_time",
                when(col("arrives") == "Source", lit(None)).otherwise(col("arrives")))
            .withColumn("departure_time",
                when(col("departs") == "Destination", lit(None)).otherwise(col("departs")))

            # 7. Add audit column
            .withColumn("ingested_at", current_timestamp())

            # 8. Select final silver schema
            .select(
                "trainNumber", "trainName", "route", "origin", "destination",
                "MON", "TUE", "WED", "THU", "FRI", "SAT", "SUN",
                "stop_number", "station_name_clean", "station_code",
                "arrival_time", "departure_time", "day", "distance_km",
                "is_source", "is_destination", "ingested_at"
            )
            .withColumnRenamed("station_name_clean", "station_name")
        )

        print(f"✔ Cleansing applied: {len(self.df_silver.columns)} columns")
        return self

    # ------------------------------------------------------------------ #
    #  Step 3: Quarantine bad records                                      #
    # ------------------------------------------------------------------ #

    def apply_quarantine(self):
        """Separate rows with critical nulls into quarantine."""
        if self.df_silver is None:
            raise RuntimeError("Call cleanse_and_transform() before apply_quarantine()")

        quarantine_condition = (
            col("trainNumber").isNull() |
            col("station_name").isNull() | (col("station_name") == "") |
            col("stop_number").isNull() |
            col("distance_km").isNull()
        )

        self.df_quarantine = self.df_silver.filter(quarantine_condition)
        self.df_silver_clean = self.df_silver.filter(~quarantine_condition)

        clean_count = self.df_silver_clean.count()
        quarantine_count = self.df_quarantine.count()
        print(f"✔ Silver clean rows : {clean_count:,}")
        print(f"  Quarantine rows   : {quarantine_count:,}")
        return self

    # ------------------------------------------------------------------ #
    #  Step 4: Write Silver and Quarantine to Delta                        #
    # ------------------------------------------------------------------ #

    def write_to_delta(self):
        """Write clean silver and quarantine DataFrames to ADLS Delta."""
        if self.df_silver_clean is None:
            raise RuntimeError("Call apply_quarantine() before write_to_delta()")

        # Write silver clean data
        self.df_silver_clean.write.format("delta").mode("overwrite").save(self.silver_path)
        print(f"✔ Silver written to: {self.silver_path}")

        # Write quarantine data
        self.df_quarantine.write.format("delta").mode("overwrite").save(self.quarantine_path)
        print(f"✔ Quarantine written to: {self.quarantine_path}")

        # Verify read-back
        silver_count = self.spark.read.format("delta").load(self.silver_path).count()
        quarantine_count = self.spark.read.format("delta").load(self.quarantine_path).count()
        bronze_count = self.df_bronze.count()

        print(f"\n{'='*50}")
        print(f"  Silver Layer Summary")
        print(f"{'='*50}")
        print(f"  Bronze input      : {bronze_count:,} rows")
        print(f"  Silver clean      : {silver_count:,} rows")
        print(f"  Quarantine        : {quarantine_count:,} rows")
        print(f"  Total accounted   : {silver_count + quarantine_count:,} rows")
        print(f"{'='*50}")
        return self

    # ------------------------------------------------------------------ #
    #  Step 5: Read Silver (for downstream layers)                         #
    # ------------------------------------------------------------------ #

    def read_silver(self):
        """Read the Silver Delta table back from ADLS."""
        df = self.spark.read.format("delta").load(self.silver_path)
        print(f"✔ Silver read-back: {df.count():,} rows from {self.silver_path}")
        return df

    # ------------------------------------------------------------------ #
    #  Orchestrator: run full Silver pipeline                              #
    # ------------------------------------------------------------------ #

    def run(self):
        """Execute the full Silver pipeline: read → cleanse → quarantine → write."""
        print("=" * 55)
        print("  Silver Layer - Trains Pipeline")
        print("=" * 55)

        self.read_bronze()
        self.cleanse_and_transform()
        self.apply_quarantine()
        self.write_to_delta()

        print("=" * 55)
        print("  Silver Layer Complete")
        print("=" * 55)
        return self.df_silver_clean
