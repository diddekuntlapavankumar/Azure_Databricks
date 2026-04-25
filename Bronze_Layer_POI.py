# Databricks notebook source
# Bronze Layer - POI ETL
#
# Author: Pavan Kumar Reddy Duddekuntla
# Role: Senior Data Engineer
#
# Overview:
#   Bronze layer module for the Points of Interest (POI) data pipeline.
#   Reads raw Parquet from ADLS Gen2, adds ingestion metadata columns,
#   and writes to Bronze Delta.
#
# Input  : ainput1/train.parquet (POI data with 13 columns)
# Output : medallion/bronze/poi (Delta - raw with audit columns)


from pyspark.sql.functions import current_timestamp, lit


class BronzeLayerPOI:
    """Handles Bronze layer ingestion: read raw Parquet, add metadata, and persist."""

    def __init__(self, manager, spark):
        """Initialize with ADLSManager instance and Spark session."""
        self.manager = manager
        self.spark = spark
        self.df_raw = None
        self.df_bronze = None
        self.bronze_path = manager.get_path(
            manager.config["medallion_paths"]["bronze"]
        )

    # ------------------------------------------------------------------ #
    #  Step 1: Read raw Parquet from ADLS                                  #
    # ------------------------------------------------------------------ #

    def read_raw_parquet(self):
        """Read the source Parquet file from ADLS Gen2."""
        self.df_raw = self.manager.read_file(
            file_path=self.manager.file_cfg["parquet"],
            file_format="parquet"
        )
        self.manager.dataframes["parquet"] = self.df_raw

        row_count = self.df_raw.count()
        print(f"\u2714 Raw Parquet loaded: {row_count:,} records")
        print(f"  Columns: {self.df_raw.columns}")
        return self

    # ------------------------------------------------------------------ #
    #  Step 2: Add ingestion metadata columns                              #
    # ------------------------------------------------------------------ #

    def add_metadata(self):
        """Add audit/metadata columns to the raw DataFrame."""
        if self.df_raw is None:
            raise RuntimeError("Call read_raw_parquet() before add_metadata()")

        self.df_bronze = (
            self.df_raw
            .withColumn("ingested_at", current_timestamp())
            .withColumn("source_file", lit("ainput1/train.parquet"))
            .withColumn("layer", lit("bronze"))
        )

        col_count = len(self.df_bronze.columns)
        print(f"\u2714 Metadata added: {col_count} columns (added ingested_at, source_file, layer)")
        return self

    # ------------------------------------------------------------------ #
    #  Step 3: Write to Bronze Delta layer                                 #
    # ------------------------------------------------------------------ #

    def write_to_delta(self):
        """Write DataFrame with metadata to Bronze Delta on ADLS Gen2."""
        if self.df_bronze is None:
            raise RuntimeError("Call add_metadata() before write_to_delta()")

        print(f"Bronze target: {self.bronze_path}")
        self.df_bronze.write.format("delta").mode("overwrite").save(self.bronze_path)

        # Verify by reading back
        df_verify = self.spark.read.format("delta").load(self.bronze_path)
        verify_count = df_verify.count()

        print(f"\u2714 Bronze Delta written successfully")
        print(f"  Rows: {verify_count:,} | Columns: {len(self.df_bronze.columns)}")
        return self

    # ------------------------------------------------------------------ #
    #  Step 4: Read Bronze (for downstream layers)                         #
    # ------------------------------------------------------------------ #

    def read_bronze(self):
        """Read the Bronze Delta table back from ADLS."""
        df = self.spark.read.format("delta").load(self.bronze_path)
        print(f"\u2714 Bronze read-back: {df.count():,} rows from {self.bronze_path}")
        return df

    # ------------------------------------------------------------------ #
    #  Orchestrator: run full Bronze pipeline                              #
    # ------------------------------------------------------------------ #

    def run(self):
        """Execute the full Bronze pipeline: read \u2192 metadata \u2192 write."""
        print("=" * 55)
        print("  Bronze Layer - POI Pipeline")
        print("=" * 55)

        self.read_raw_parquet()
        self.add_metadata()
        self.write_to_delta()

        print("=" * 55)
        print("  Bronze Layer Complete")
        print("=" * 55)
        return self.df_bronze
