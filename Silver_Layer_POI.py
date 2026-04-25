# Databricks notebook source
# Silver Layer - POI ETL
#
# Author: Pavan Kumar Reddy Duddekuntla
# Role: Senior Data Engineer
#
# Overview:
#   Silver layer module for the Points of Interest (POI) data pipeline.
#   Reads Bronze Delta, applies data cleansing and validation,
#   quarantines bad records, and writes clean data to Silver Delta.
#
# Input  : medallion/bronze/poi (Delta - raw with audit columns)
# Output : medallion/silver/poi (Delta - cleansed records)
#          medallion/bronze/poi_quarantine (Delta - rejected rows)


from pyspark.sql.functions import (
    col, trim, when, lit, current_timestamp, upper, length,
    regexp_replace
)
from pyspark.sql.types import DoubleType


class SilverLayerPOI:
    """Handles Silver layer cleansing: validation, normalization, quarantine, and persist."""

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
        print(f"\u2714 Bronze loaded: {row_count:,} rows")
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

            # 1. Trim whitespace on all string columns
            .withColumn("id",       trim(col("id")))
            .withColumn("name",     trim(col("name")))
            .withColumn("address",  trim(col("address")))
            .withColumn("city",     trim(col("city")))
            .withColumn("state",    trim(col("state")))
            .withColumn("zip",      trim(col("zip")))
            .withColumn("country",  trim(col("country")))
            .withColumn("url",      trim(col("url")))
            .withColumn("phone",    trim(col("phone")))
            .withColumn("categories",       trim(col("categories")))
            .withColumn("point_of_interest", trim(col("point_of_interest")))

            # 2. Normalize country code to uppercase (2-letter ISO)
            .withColumn("country", upper(col("country")))

            # 3. Cast latitude/longitude to double (safety)
            .withColumn("latitude",  col("latitude").cast(DoubleType()))
            .withColumn("longitude", col("longitude").cast(DoubleType()))

            # 4. Clean phone: remove non-digit characters for standardization
            .withColumn("phone_clean",
                regexp_replace(col("phone"), r"[^\d+]", ""))

            # 5. Flag completeness: address available?
            .withColumn("has_address",
                when(col("address").isNotNull() & (length(col("address")) > 0),
                     lit(True)).otherwise(lit(False)))

            # 6. Flag completeness: contact info available?
            .withColumn("has_contact",
                when(
                    col("phone").isNotNull() | col("url").isNotNull(),
                    lit(True)
                ).otherwise(lit(False)))

            # 7. Add audit column
            .withColumn("silver_ingested_at", current_timestamp())

            # 8. Select final silver schema
            .select(
                "id", "name", "latitude", "longitude",
                "address", "city", "state", "zip", "country",
                "url", "phone", "phone_clean",
                "categories", "point_of_interest",
                "has_address", "has_contact",
                "ingested_at", "silver_ingested_at"
            )
        )

        print(f"\u2714 Cleansing applied: {len(self.df_silver.columns)} columns")
        return self

    # ------------------------------------------------------------------ #
    #  Step 3: Quarantine bad records                                      #
    # ------------------------------------------------------------------ #

    def apply_quarantine(self):
        """Separate rows with critical data quality issues into quarantine."""
        if self.df_silver is None:
            raise RuntimeError("Call cleanse_and_transform() before apply_quarantine()")

        quarantine_condition = (
            # Missing required ID
            col("id").isNull() | (col("id") == "") |
            # Missing name
            col("name").isNull() | (col("name") == "") |
            # Invalid latitude (must be -90 to 90)
            col("latitude").isNull() |
            (col("latitude") < -90) | (col("latitude") > 90) |
            # Invalid longitude (must be -180 to 180)
            col("longitude").isNull() |
            (col("longitude") < -180) | (col("longitude") > 180) |
            # Missing country code
            col("country").isNull() | (col("country") == "")
        )

        self.df_quarantine = self.df_silver.filter(quarantine_condition)
        self.df_silver_clean = self.df_silver.filter(~quarantine_condition)

        clean_count = self.df_silver_clean.count()
        quarantine_count = self.df_quarantine.count()
        print(f"\u2714 Silver clean rows : {clean_count:,}")
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
        print(f"\u2714 Silver written to: {self.silver_path}")

        # Write quarantine data
        self.df_quarantine.write.format("delta").mode("overwrite").save(self.quarantine_path)
        print(f"\u2714 Quarantine written to: {self.quarantine_path}")

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
        print(f"\u2714 Silver read-back: {df.count():,} rows from {self.silver_path}")
        return df

    # ------------------------------------------------------------------ #
    #  Orchestrator: run full Silver pipeline                              #
    # ------------------------------------------------------------------ #

    def run(self):
        """Execute the full Silver pipeline: read \u2192 cleanse \u2192 quarantine \u2192 write."""
        print("=" * 55)
        print("  Silver Layer - POI Pipeline")
        print("=" * 55)

        self.read_bronze()
        self.cleanse_and_transform()
        self.apply_quarantine()
        self.write_to_delta()

        print("=" * 55)
        print("  Silver Layer Complete")
        print("=" * 55)
        return self.df_silver_clean
