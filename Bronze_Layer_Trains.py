# Databricks notebook source
# Bronze Layer - Trains ETL
#
# Author: Pavan Kumar Reddy Duddekuntla
# Role: Senior Data Engineer
#
# Overview:
#   Bronze layer module for the Indian Railways train data pipeline.
#   Reads raw JSON from ADLS Gen2, flattens the nested trainRoute
#   array into individual stop records, and writes to Bronze Delta.
#
# Input  : ainput1/EXP-TRAINS.json (nested JSON with trainRoute array)
# Output : medallion/bronze/exp_trains (Delta - one row per stop)


from pyspark.sql.functions import explode, col


class BronzeLayerTrains:
    """Handles Bronze layer ingestion: read raw JSON, flatten, and persist."""

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
    #  Step 1: Read raw JSON from ADLS                                     #
    # ------------------------------------------------------------------ #

    def read_raw_json(self):
        """Read the source JSON file from ADLS Gen2."""
        self.df_raw = self.manager.read_file(
            file_path=self.manager.file_cfg["json"],
            file_format="json",
            multiLine=True
        )
        self.manager.dataframes["json"] = self.df_raw

        row_count = self.df_raw.count()
        print(f"✔ Raw JSON loaded: {row_count:,} trains")
        print(f"  Columns: {self.df_raw.columns}")
        return self

    # ------------------------------------------------------------------ #
    #  Step 2: Flatten trainRoute array into individual stops              #
    # ------------------------------------------------------------------ #

    def flatten_train_routes(self):
        """Explode nested trainRoute array so each stop becomes its own row."""
        if self.df_raw is None:
            raise RuntimeError("Call read_raw_json() before flatten_train_routes()")

        self.df_bronze = (
            self.df_raw
            .select(
                col("trainNumber"),
                col("trainName"),
                col("route"),
                col("runningDays.MON").alias("MON"),
                col("runningDays.TUE").alias("TUE"),
                col("runningDays.WED").alias("WED"),
                col("runningDays.THU").alias("THU"),
                col("runningDays.FRI").alias("FRI"),
                col("runningDays.SAT").alias("SAT"),
                col("runningDays.SUN").alias("SUN"),
                explode(col("trainRoute")).alias("stop")
            )
            .select(
                "trainNumber",
                "trainName",
                "route",
                "MON", "TUE", "WED", "THU", "FRI", "SAT", "SUN",
                col("stop.sno").alias("stop_number"),
                col("stop.stationName").alias("station_name"),
                col("stop.arrives").alias("arrives"),
                col("stop.departs").alias("departs"),
                col("stop.day").alias("day"),
                col("stop.distance").alias("distance")
            )
        )

        stop_count = self.df_bronze.count()
        print(f"✔ Flattened to: {stop_count:,} individual stops")
        print(f"  Columns: {self.df_bronze.columns}")
        return self

    # ------------------------------------------------------------------ #
    #  Step 3: Write to Bronze Delta layer                                 #
    # ------------------------------------------------------------------ #

    def write_to_delta(self):
        """Write flattened DataFrame to Bronze Delta on ADLS Gen2."""
        if self.df_bronze is None:
            raise RuntimeError("Call flatten_train_routes() before write_to_delta()")

        print(f"Bronze target: {self.bronze_path}")
        self.df_bronze.write.format("delta").mode("overwrite").save(self.bronze_path)

        # Verify by reading back
        df_verify = self.spark.read.format("delta").load(self.bronze_path)
        verify_count = df_verify.count()

        print(f"✔ Bronze Delta written successfully")
        print(f"  Rows: {verify_count:,} | Columns: {len(self.df_bronze.columns)}")
        return self

    # ------------------------------------------------------------------ #
    #  Step 4: Read Bronze (for downstream layers)                         #
    # ------------------------------------------------------------------ #

    def read_bronze(self):
        """Read the Bronze Delta table back from ADLS."""
        df = self.spark.read.format("delta").load(self.bronze_path)
        print(f"✔ Bronze read-back: {df.count():,} rows from {self.bronze_path}")
        return df

    # ------------------------------------------------------------------ #
    #  Orchestrator: run full Bronze pipeline                              #
    # ------------------------------------------------------------------ #

    def run(self):
        """Execute the full Bronze pipeline: read → flatten → write."""
        print("=" * 55)
        print("  Bronze Layer - Trains Pipeline")
        print("=" * 55)

        self.read_raw_json()
        self.flatten_train_routes()
        self.write_to_delta()

        print("=" * 55)
        print("  Bronze Layer Complete")
        print("=" * 55)
        return self.df_bronze
