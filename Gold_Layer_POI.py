# Databricks notebook source
# Gold Layer - POI ETL
#
# Author: Pavan Kumar Reddy Duddekuntla
# Role: Senior Data Engineer
#
# Overview:
#   Gold layer module for the Points of Interest (POI) data pipeline.
#   Reads Silver Delta and produces three aggregated gold tables:
#     1. Country Summary         - POI count, completeness stats per country
#     2. Category Distribution   - POI count per category with percentages
#     3. City Density            - POI count per city/country with coordinates
#
# Input  : medallion/silver/poi (Delta - cleansed records)
# Output : medallion/gold/poi_country_summary (Delta)
#          medallion/gold/poi_category_distribution (Delta)
#          medallion/gold/poi_city_density (Delta)


from pyspark.sql.functions import (
    col, count, countDistinct, sum as spark_sum,
    avg as spark_avg, round as spark_round, lit,
    when, desc
)
from pyspark.sql import Window


class GoldLayerPOI:
    """Handles Gold layer aggregations: country summary, category distribution, city density."""

    def __init__(self, manager, spark):
        """Initialize with ADLSManager instance and Spark session."""
        self.manager = manager
        self.spark = spark
        self.df_silver = None
        self.df_country_summary = None
        self.df_category_dist = None
        self.df_city_density = None
        self.silver_path = manager.get_path(
            manager.config["medallion_paths"]["silver"]
        )
        self.gold_country_path = manager.get_path(
            manager.config["medallion_paths"]["gold_country_summary"]
        )
        self.gold_category_path = manager.get_path(
            manager.config["medallion_paths"]["gold_category_distribution"]
        )
        self.gold_city_path = manager.get_path(
            manager.config["medallion_paths"]["gold_city_density"]
        )

    # ------------------------------------------------------------------ #
    #  Step 1: Read Silver Delta layer                                     #
    # ------------------------------------------------------------------ #

    def read_silver(self):
        """Read the Silver Delta table from ADLS."""
        self.df_silver = self.spark.read.format("delta").load(self.silver_path)
        row_count = self.df_silver.count()
        print(f"\u2714 Silver loaded: {row_count:,} rows")
        return self

    # ------------------------------------------------------------------ #
    #  Step 2a: Country Summary aggregation                                #
    # ------------------------------------------------------------------ #

    def build_country_summary(self):
        """Aggregate POI metrics per country."""
        if self.df_silver is None:
            raise RuntimeError("Call read_silver() before build_country_summary()")

        total_count = self.df_silver.count()

        self.df_country_summary = (
            self.df_silver
            .groupBy("country")
            .agg(
                count("*").alias("poi_count"),
                countDistinct("city").alias("unique_cities"),
                countDistinct("categories").alias("unique_categories"),
                spark_round(spark_avg("latitude"), 4).alias("avg_latitude"),
                spark_round(spark_avg("longitude"), 4).alias("avg_longitude"),
                spark_sum(when(col("has_address") == True, 1).otherwise(0)).alias("with_address"),
                spark_sum(when(col("has_contact") == True, 1).otherwise(0)).alias("with_contact"),
            )
            .withColumn("pct_of_total",
                spark_round(col("poi_count") * 100.0 / lit(total_count), 2))
            .withColumn("address_completeness_pct",
                spark_round(col("with_address") * 100.0 / col("poi_count"), 2))
            .withColumn("contact_completeness_pct",
                spark_round(col("with_contact") * 100.0 / col("poi_count"), 2))
            .orderBy(desc("poi_count"))
        )

        row_count = self.df_country_summary.count()
        print(f"\u2714 Country Summary built: {row_count:,} countries")
        return self

    # ------------------------------------------------------------------ #
    #  Step 2b: Category Distribution aggregation                          #
    # ------------------------------------------------------------------ #

    def build_category_distribution(self):
        """Count POI per category with percentage of total."""
        if self.df_silver is None:
            raise RuntimeError("Call read_silver() before build_category_distribution()")

        total_count = self.df_silver.count()

        self.df_category_dist = (
            self.df_silver
            .filter(col("categories").isNotNull() & (col("categories") != ""))
            .groupBy("categories")
            .agg(
                count("*").alias("poi_count"),
                countDistinct("country").alias("countries_present"),
                countDistinct("city").alias("unique_cities"),
            )
            .withColumn("pct_of_total",
                spark_round(col("poi_count") * 100.0 / lit(total_count), 2))
            .orderBy(desc("poi_count"))
        )

        row_count = self.df_category_dist.count()
        print(f"\u2714 Category Distribution built: {row_count:,} categories")
        return self

    # ------------------------------------------------------------------ #
    #  Step 2c: City Density aggregation                                   #
    # ------------------------------------------------------------------ #

    def build_city_density(self):
        """Count POI per city with average coordinates for mapping."""
        if self.df_silver is None:
            raise RuntimeError("Call read_silver() before build_city_density()")

        self.df_city_density = (
            self.df_silver
            .filter(col("city").isNotNull() & (col("city") != ""))
            .groupBy("city", "state", "country")
            .agg(
                count("*").alias("poi_count"),
                countDistinct("categories").alias("unique_categories"),
                spark_round(spark_avg("latitude"), 6).alias("center_latitude"),
                spark_round(spark_avg("longitude"), 6).alias("center_longitude"),
            )
            .orderBy(desc("poi_count"))
        )

        row_count = self.df_city_density.count()
        print(f"\u2714 City Density built: {row_count:,} cities")
        return self

    # ------------------------------------------------------------------ #
    #  Step 3: Write all Gold tables to Delta                              #
    # ------------------------------------------------------------------ #

    def write_to_delta(self):
        """Write all gold DataFrames to ADLS Delta with verification."""
        results = {}

        for name, df, path in [
            ("Country Summary",        self.df_country_summary,  self.gold_country_path),
            ("Category Distribution",  self.df_category_dist,    self.gold_category_path),
            ("City Density",           self.df_city_density,     self.gold_city_path),
        ]:
            if df is None:
                print(f"\u26a0 Skipping {name}: not built yet")
                continue

            df.write.format("delta").mode("overwrite").save(path)
            verify_count = self.spark.read.format("delta").load(path).count()
            results[name] = verify_count
            print(f"\u2714 {name} written: {verify_count:,} rows \u2192 {path}")

        # Summary
        print(f"\n{'='*55}")
        print(f"  Gold Layer Summary")
        print(f"{'='*55}")
        for name, cnt in results.items():
            print(f"  {name:<25}: {cnt:,} rows")
        print(f"{'='*55}")
        return self

    # ------------------------------------------------------------------ #
    #  Read helpers (for downstream use)                                   #
    # ------------------------------------------------------------------ #

    def read_country_summary(self):
        """Read the Gold country summary Delta table."""
        df = self.spark.read.format("delta").load(self.gold_country_path)
        print(f"\u2714 Country Summary read: {df.count():,} rows")
        return df

    def read_category_distribution(self):
        """Read the Gold category distribution Delta table."""
        df = self.spark.read.format("delta").load(self.gold_category_path)
        print(f"\u2714 Category Distribution read: {df.count():,} rows")
        return df

    def read_city_density(self):
        """Read the Gold city density Delta table."""
        df = self.spark.read.format("delta").load(self.gold_city_path)
        print(f"\u2714 City Density read: {df.count():,} rows")
        return df

    # ------------------------------------------------------------------ #
    #  Orchestrator: run full Gold pipeline                                #
    # ------------------------------------------------------------------ #

    def run(self):
        """Execute the full Gold pipeline: read \u2192 aggregate all \u2192 write."""
        print("=" * 55)
        print("  Gold Layer - POI Pipeline")
        print("=" * 55)

        self.read_silver()
        self.build_country_summary()
        self.build_category_distribution()
        self.build_city_density()
        self.write_to_delta()

        print("=" * 55)
        print("  Gold Layer Complete")
        print("=" * 55)
        return {
            "country_summary": self.df_country_summary,
            "category_distribution": self.df_category_dist,
            "city_density": self.df_city_density,
        }
