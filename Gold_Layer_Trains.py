# Databricks notebook source
# Gold Layer - Trains ETL
#
# Author: Pavan Kumar Reddy Duddekuntla
# Role: Senior Data Engineer
#
# Overview:
#   Gold layer module for the Indian Railways train data pipeline.
#   Reads Silver Delta and produces three aggregated gold tables:
#     1. Route Summary       - one row per train route with distance, stops, schedule
#     2. Station Frequency   - how many trains stop at each station
#     3. Running Days        - train counts by day of week
#
# Input  : medallion/silver/exp_trains (Delta - cleansed stops)
# Output : medallion/gold/train_route_summary (Delta)
#          medallion/gold/train_station_frequency (Delta)
#          medallion/gold/train_running_days (Delta)


from pyspark.sql.functions import (
    col, count, countDistinct, sum as spark_sum,
    max as spark_max, round as spark_round, lit
)


class GoldLayerTrains:
    """Handles Gold layer aggregations: route summary, station frequency, running days."""

    def __init__(self, manager, spark):
        """Initialize with ADLSManager instance and Spark session."""
        self.manager = manager
        self.spark = spark
        self.df_silver = None
        self.df_route_summary = None
        self.df_station_freq = None
        self.df_running_days = None
        self.silver_path = manager.get_path(
            manager.config["medallion_paths"]["silver"]
        )
        self.gold_route_path = manager.get_path(
            manager.config["medallion_paths"]["gold_route_summary"]
        )
        self.gold_station_path = manager.get_path(
            manager.config["medallion_paths"]["gold_station_frequency"]
        )
        self.gold_days_path = manager.get_path(
            manager.config["medallion_paths"]["gold_running_days"]
        )

    # ------------------------------------------------------------------ #
    #  Step 1: Read Silver Delta layer                                     #
    # ------------------------------------------------------------------ #

    def read_silver(self):
        """Read the Silver Delta table from ADLS."""
        self.df_silver = self.spark.read.format("delta").load(self.silver_path)
        row_count = self.df_silver.count()
        print(f"✔ Silver loaded: {row_count:,} rows")
        return self

    # ------------------------------------------------------------------ #
    #  Step 2a: Route Summary aggregation                                  #
    # ------------------------------------------------------------------ #

    def build_route_summary(self):
        """Aggregate one row per train route with distance, stops, and schedule."""
        if self.df_silver is None:
            raise RuntimeError("Call read_silver() before build_route_summary()")

        self.df_route_summary = 
        (
            self.df_silver
            .groupBy("trainNumber", "trainName", "route", "origin", "destination")
            .agg
            (
                count("stop_number").alias("total_stops"),
                spark_max("distance_km").alias("total_distance_km"),
                spark_max("stop_number").alias("max_stop_number"),
                countDistinct("station_name").alias("unique_stations"),
                spark_max("day").alias("journey_days"),

                # Running days: take max (all rows per train are identical)
                spark_max(col("MON").cast("int")).alias("MON"),
                spark_max(col("TUE").cast("int")).alias("TUE"),
                spark_max(col("WED").cast("int")).alias("WED"),
                spark_max(col("THU").cast("int")).alias("THU"),
                spark_max(col("FRI").cast("int")).alias("FRI"),
                spark_max(col("SAT").cast("int")).alias("SAT"),
                spark_max(col("SUN").cast("int")).alias("SUN"),
            )
            # Compute days per week the train runs
            .withColumn("days_per_week",
                col("MON") + col("TUE") + col("WED") +
                col("THU") + col("FRI") + col("SAT") + col("SUN")
            )
            # Average distance between stops
            .withColumn("avg_km_per_stop",
                spark_round(col("total_distance_km") / col("total_stops"), 2)
            )
            .orderBy(col("total_distance_km").desc())
        )

        row_count = self.df_route_summary.count()
        print(f"✔ Route Summary built: {row_count:,} routes")
        return self

    # ------------------------------------------------------------------ #
    #  Step 2b: Station Frequency aggregation                              #
    # ------------------------------------------------------------------ #

    def build_station_frequency(self):
        """Count how many distinct trains stop at each station."""
        if self.df_silver is None:
            raise RuntimeError("Call read_silver() before build_station_frequency()")

        self.df_station_freq = 
        (
            self.df_silver
            .groupBy("station_name", "station_code")
            .agg
            (
                countDistinct("trainNumber").alias("train_count"),
                count("*").alias("total_stop_events"),
                countDistinct("route").alias("unique_routes"),
            )
            .orderBy(col("train_count").desc())
        )

        row_count = self.df_station_freq.count()
        print(f"✔ Station Frequency built: {row_count:,} stations")
        return self

    # ------------------------------------------------------------------ #
    #  Step 2c: Running Days aggregation                                   #
    # ------------------------------------------------------------------ #

    def build_running_days(self):
        """Summarize train counts by day of week (unpivot MON–SUN)."""
        if self.df_silver is None:
            raise RuntimeError("Call read_silver() before build_running_days()")

        # Get one row per train for accurate day counts
        df_trains = 
        (
            self.df_silver
            .select
            (
                "trainNumber",
                col("MON").cast("int").alias("MON"),
                col("TUE").cast("int").alias("TUE"),
                col("WED").cast("int").alias("WED"),
                col("THU").cast("int").alias("THU"),
                col("FRI").cast("int").alias("FRI"),
                col("SAT").cast("int").alias("SAT"),
                col("SUN").cast("int").alias("SUN"),
            )
            .dropDuplicates(["trainNumber"])
        )

        # Unpivot days using stack
        self.df_running_days = 
        (
            df_trains
            .select
            (
                "trainNumber",
                self.spark.sql("""SELECT 1""").schema  # placeholder avoided
            )
        )

        # Use SQL stack for clean unpivot
        df_trains.createOrReplaceTempView("_gold_trains_days")
        self.df_running_days = self.spark.sql("""
            SELECT day_name, 
                   SUM(runs) AS train_count,
                   ROUND(SUM(runs) * 100.0 / COUNT(*), 2) AS pct_of_trains
            FROM 
            (
                SELECT trainNumber,
                       STACK
                       (7,
                           'MON', MON,
                           'TUE', TUE,
                           'WED', WED,
                           'THU', THU,
                           'FRI', FRI,
                           'SAT', SAT,
                           'SUN', SUN
                       ) AS (day_name, runs)
                FROM _gold_trains_days
            )
            GROUP BY day_name
            ORDER BY CASE day_name
                WHEN 'MON' THEN 1 WHEN 'TUE' THEN 2 WHEN 'WED' THEN 3
                WHEN 'THU' THEN 4 WHEN 'FRI' THEN 5 WHEN 'SAT' THEN 6
                WHEN 'SUN' THEN 7 END
        """)

        print(f"✔ Running Days built: {self.df_running_days.count()} day rows")
        return self

    # ------------------------------------------------------------------ #
    #  Step 3: Write all Gold tables to Delta                              #
    # ------------------------------------------------------------------ #

    def write_to_delta(self):
        """Write all gold DataFrames to ADLS Delta with verification."""
        results = {}

        for name, df, path in [
            ("Route Summary",      self.df_route_summary,  self.gold_route_path),
            ("Station Frequency",  self.df_station_freq,   self.gold_station_path),
            ("Running Days",       self.df_running_days,   self.gold_days_path),
        ]:
            if df is None:
                print(f"⚠ Skipping {name}: not built yet")
                continue

            df.write.format("delta").mode("overwrite").save(path)
            verify_count = self.spark.read.format("delta").load(path).count()
            results[name] = verify_count
            print(f"✔ {name} written: {verify_count:,} rows → {path}")

        # Summary
        print(f"\n{'='*55}")
        print(f"  Gold Layer Summary")
        print(f"{'='*55}")
        for name, cnt in results.items():
            print(f"  {name:<22}: {cnt:,} rows")
        print(f"{'='*55}")
        return self

    # ------------------------------------------------------------------ #
    #  Read helpers (for downstream use)                                   #
    # ------------------------------------------------------------------ #

    def read_route_summary(self):
        """Read the Gold route summary Delta table."""
        df = self.spark.read.format("delta").load(self.gold_route_path)
        print(f"✔ Route Summary read: {df.count():,} rows")
        return df

    def read_station_frequency(self):
        """Read the Gold station frequency Delta table."""
        df = self.spark.read.format("delta").load(self.gold_station_path)
        print(f"✔ Station Frequency read: {df.count():,} rows")
        return df

    def read_running_days(self):
        """Read the Gold running days Delta table."""
        df = self.spark.read.format("delta").load(self.gold_days_path)
        print(f"✔ Running Days read: {df.count():,} rows")
        return df

    # ------------------------------------------------------------------ #
    #  Orchestrator: run full Gold pipeline                                #
    # ------------------------------------------------------------------ #

    def run(self):
        """Execute the full Gold pipeline: read → aggregate all → write."""
        print("=" * 55)
        print("  Gold Layer - Trains Pipeline")
        print("=" * 55)

        self.read_silver()
        self.build_route_summary()
        self.build_station_frequency()
        self.build_running_days()
        self.write_to_delta()

        print("=" * 55)
        print("  Gold Layer Complete")
        print("=" * 55)
        return {
            "route_summary": self.df_route_summary,
            "station_frequency": self.df_station_freq,
            "running_days": self.df_running_days,
        }
