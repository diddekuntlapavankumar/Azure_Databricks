"""Healthcare Medallion Pipeline — Production Module.

End-to-end ETL pipeline that reads ``healthcare_dataset.csv`` from
Azure Data Lake Storage Gen2 and processes it through a Bronze → Silver → Gold
medallion architecture inside Unity Catalog.

Author : Pavan Kumar Reddy Duddekuntla
Role   : Senior Data Engineer

Usage (from a Databricks notebook)::

    from healthcare_medallion_pipeline import HealthcareMedallionPipeline

    pipeline = HealthcareMedallionPipeline(adls_manager=adls, spark=spark, display_fn=display)
    summary  = pipeline.run()
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Optional

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# ---------------------------------------------------------------------------
# Module-level logger
# ---------------------------------------------------------------------------
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

if not logger.handlers:
    _handler = logging.StreamHandler()
    _handler.setFormatter(
        logging.Formatter("%(asctime)s | %(levelname)-7s | %(name)s | %(message)s")
    )
    logger.addHandler(_handler)


# ======================================================================
# Configuration
# ======================================================================

@dataclass(frozen=True)
class PipelineConfig:
    """Immutable configuration for the medallion pipeline."""

    # Unity Catalog coordinates
    catalog: str = "azuredatabricks1"
    bronze_schema: str = "bronze"
    silver_schema: str = "silver"
    gold_schema: str = "gold"

    # Table names
    bronze_table: str = "healthcare_raw"
    silver_table: str = "healthcare_cleaned"
    gold_condition_table: str = "healthcare_condition_summary"
    gold_admission_table: str = "healthcare_admission_summary"
    gold_hospital_table: str = "healthcare_hospital_performance"

    # Source file on ADLS
    source_file_path: str = "ainput1/healthcare_dataset.csv"
    source_file_format: str = "csv"

    # Column mapping: raw CSV header → snake_case
    column_mapping: Dict[str, str] = field(default_factory=lambda: {
        "Name": "name",
        "Age": "age",
        "Gender": "gender",
        "Blood Type": "blood_type",
        "Medical Condition": "medical_condition",
        "Date of Admission": "admission_date",
        "Doctor": "doctor",
        "Hospital": "hospital",
        "Insurance Provider": "insurance_provider",
        "Billing Amount": "billing_amount",
        "Room Number": "room_number",
        "Admission Type": "admission_type",
        "Discharge Date": "discharge_date",
        "Medication": "medication",
        "Test Results": "test_results",
    })

    # Silver deduplication keys
    dedup_partition_cols: List[str] = field(default_factory=lambda: [
        "name", "age", "gender", "admission_date", "medical_condition",
    ])
    dedup_order_col: str = "billing_amount"

    # --- helpers -----------------------------------------------------------

    def fqn(self, schema: str, table: str) -> str:
        """Return a fully-qualified ``catalog.schema.table`` name."""
        return f"{self.catalog}.{schema}.{table}"

    @property
    def bronze_fqn(self) -> str:
        return self.fqn(self.bronze_schema, self.bronze_table)

    @property
    def silver_fqn(self) -> str:
        return self.fqn(self.silver_schema, self.silver_table)

    @property
    def gold_condition_fqn(self) -> str:
        return self.fqn(self.gold_schema, self.gold_condition_table)

    @property
    def gold_admission_fqn(self) -> str:
        return self.fqn(self.gold_schema, self.gold_admission_table)

    @property
    def gold_hospital_fqn(self) -> str:
        return self.fqn(self.gold_schema, self.gold_hospital_table)


# ======================================================================
# Pipeline
# ======================================================================

class HealthcareMedallionPipeline:
    """Orchestrates the Bronze → Silver → Gold medallion pipeline.

    Parameters
    ----------
    adls_manager : ADLSManager
        An already-authenticated ``ADLSManager`` instance.
    spark : SparkSession
        The active Spark session.
    display_fn : callable, optional
        The notebook ``display()`` function for interactive output.
    config : PipelineConfig, optional
        Override default pipeline configuration.
    """

    def __init__(
        self,
        adls_manager: Any,
        spark: SparkSession,
        display_fn: Optional[Callable] = None,
        config: Optional[PipelineConfig] = None,
    ) -> None:
        self.adls = adls_manager
        self.spark = spark
        self.display = display_fn or (lambda df: None)
        self.cfg = config or PipelineConfig()
        self._row_counts: Dict[str, int] = {}

    # ------------------------------------------------------------------
    # Utility helpers
    # ------------------------------------------------------------------

    def _write_delta(
        self, df: DataFrame, table_fqn: str, mode: str = "overwrite"
    ) -> None:
        """Write *df* to a Delta table with schema overwrite."""
        (
            df.write
            .format("delta")
            .mode(mode)
            .option("overwriteSchema", "true")
            .saveAsTable(table_fqn)
        )
        logger.info("Written to: %s", table_fqn)

    def _verify_table(self, table_fqn: str) -> int:
        """Read a Delta table back and return its row count."""
        count = self.spark.table(table_fqn).count()
        logger.info("Verified %s — %s rows", table_fqn, f"{count:,}")
        return count

    # ==================================================================
    # Schema management
    # ==================================================================

    def setup_catalog_and_schemas(self) -> None:
        """Set the active catalog and create bronze / silver / gold schemas."""
        self.spark.sql(f"USE CATALOG {self.cfg.catalog}")
        logger.info("Using catalog: %s", self.cfg.catalog)

        for schema in (self.cfg.bronze_schema, self.cfg.silver_schema, self.cfg.gold_schema):
            self.spark.sql(f"CREATE SCHEMA IF NOT EXISTS {schema}")
            logger.info("Schema ready: %s.%s", self.cfg.catalog, schema)

    # ==================================================================
    # Bronze layer — raw ingestion
    # ==================================================================

    def _rename_columns(self, df: DataFrame) -> DataFrame:
        """Rename raw CSV headers to snake_case using the config mapping."""
        for old_name, new_name in self.cfg.column_mapping.items():
            df = df.withColumnRenamed(old_name, new_name)
        return df

    def ingest_bronze(self) -> DataFrame:
        """Read the raw CSV from ADLS and write to the bronze Delta table.

        Returns
        -------
        DataFrame
            The raw DataFrame with snake_case column names.
        """
        logger.info("Bronze: ingesting from %s", self.cfg.source_file_path)

        df_raw = self.adls.read_file(
            self.cfg.source_file_path,
            self.cfg.source_file_format,
            header="true",
            inferSchema="true",
        )
        df_bronze = self._rename_columns(df_raw)

        row_count = df_bronze.count()
        logger.info("Bronze: ingested %s rows, %d columns", f"{row_count:,}", len(df_bronze.columns))

        self._write_delta(df_bronze, self.cfg.bronze_fqn)
        self._row_counts[self.cfg.bronze_fqn] = self._verify_table(self.cfg.bronze_fqn)

        return df_bronze

    # ==================================================================
    # Silver layer — cleansing and enrichment
    # ==================================================================

    @staticmethod
    def _standardize_names(df: DataFrame) -> DataFrame:
        """Trim whitespace and apply proper-case to the *name* column."""
        return df.withColumn("name", F.initcap(F.trim(F.col("name"))))

    @staticmethod
    def _filter_invalid_billing(df: DataFrame) -> DataFrame:
        """Remove rows where ``billing_amount`` is negative."""
        before = df.count()
        df_filtered = df.filter(F.col("billing_amount") >= 0)
        removed = before - df_filtered.count()
        logger.info("Silver: removed %s rows with negative billing", f"{removed:,}")
        return df_filtered

    def _deduplicate(self, df: DataFrame) -> DataFrame:
        """Window-based dedup: keep the row with the highest billing per patient visit."""
        window = Window.partitionBy(
            *self.cfg.dedup_partition_cols
        ).orderBy(F.col(self.cfg.dedup_order_col).desc())

        df_deduped = (
            df
            .withColumn("_row_num", F.row_number().over(window))
            .filter(F.col("_row_num") == 1)
            .drop("_row_num")
        )
        logger.info("Silver: after deduplication — %s rows", f"{df_deduped.count():,}")
        return df_deduped

    @staticmethod
    def _enrich(df: DataFrame) -> DataFrame:
        """Add computed and audit columns.

        - ``length_of_stay``: days between admission and discharge.
        - ``ingestion_timestamp``: pipeline execution timestamp.
        """
        return (
            df
            .withColumn(
                "length_of_stay",
                F.datediff(F.col("discharge_date"), F.col("admission_date")),
            )
            .withColumn("ingestion_timestamp", F.current_timestamp())
        )

    def transform_silver(self) -> DataFrame:
        """Apply all Silver transformations and persist to Delta.

        Returns
        -------
        DataFrame
            The cleansed and enriched DataFrame.
        """
        logger.info("Silver: starting transformations")

        df = self.spark.table(self.cfg.bronze_fqn)
        df = self._standardize_names(df)
        df = self._filter_invalid_billing(df)
        df = self._deduplicate(df)
        df = self._enrich(df)

        logger.info(
            "Silver: transformation complete — %s rows, %d columns",
            f"{df.count():,}", len(df.columns),
        )

        self._write_delta(df, self.cfg.silver_fqn)
        self._row_counts[self.cfg.silver_fqn] = self._verify_table(self.cfg.silver_fqn)

        return df

    def verify_silver_quality(self) -> DataFrame:
        """Return a single-row DataFrame with null counts per column."""
        df = self.spark.table(self.cfg.silver_fqn)
        null_counts = df.select(
            [
                F.sum(F.when(F.isnull(c), 1).otherwise(0)).alias(c)
                for c in df.columns
            ]
        )
        return null_counts

    # ==================================================================
    # Gold layer — business-ready aggregations
    # ==================================================================

    @staticmethod
    def build_condition_summary(df: DataFrame) -> DataFrame:
        """Aggregate patient metrics by medical condition."""
        return (
            df
            .groupBy("medical_condition")
            .agg(
                F.count("*").alias("patient_count"),
                F.round(F.avg("age"), 1).alias("avg_age"),
                F.round(F.avg("billing_amount"), 2).alias("avg_billing"),
                F.round(F.min("billing_amount"), 2).alias("min_billing"),
                F.round(F.max("billing_amount"), 2).alias("max_billing"),
                F.round(F.avg("length_of_stay"), 1).alias("avg_length_of_stay"),
                F.round(F.sum("billing_amount"), 2).alias("total_billing"),
            )
            .orderBy(F.col("patient_count").desc())
        )

    @staticmethod
    def build_admission_summary(df: DataFrame) -> DataFrame:
        """Aggregate metrics by admission type and insurance provider."""
        return (
            df
            .groupBy("admission_type", "insurance_provider")
            .agg(
                F.count("*").alias("patient_count"),
                F.round(F.avg("billing_amount"), 2).alias("avg_billing"),
                F.round(F.avg("length_of_stay"), 1).alias("avg_length_of_stay"),
                F.round(F.sum("billing_amount"), 2).alias("total_billing"),
            )
            .orderBy("admission_type", F.col("patient_count").desc())
        )

    @staticmethod
    def build_hospital_performance(df: DataFrame) -> DataFrame:
        """Compute hospital-level KPIs."""
        return (
            df
            .groupBy("hospital")
            .agg(
                F.count("*").alias("total_patients"),
                F.countDistinct("doctor").alias("unique_doctors"),
                F.countDistinct("medical_condition").alias("conditions_treated"),
                F.round(F.avg("billing_amount"), 2).alias("avg_billing"),
                F.round(F.avg("length_of_stay"), 1).alias("avg_length_of_stay"),
                F.round(F.sum("billing_amount"), 2).alias("total_revenue"),
            )
            .orderBy(F.col("total_patients").desc())
        )

    def build_gold_layer(self) -> Dict[str, DataFrame]:
        """Build all three Gold aggregation tables and persist to Delta.

        Returns
        -------
        dict[str, DataFrame]
            Mapping of fully-qualified table name → DataFrame.
        """
        logger.info("Gold: building aggregation tables")

        df_silver = self.spark.table(self.cfg.silver_fqn)

        gold_tables: Dict[str, DataFrame] = {
            self.cfg.gold_condition_fqn: self.build_condition_summary(df_silver),
            self.cfg.gold_admission_fqn: self.build_admission_summary(df_silver),
            self.cfg.gold_hospital_fqn: self.build_hospital_performance(df_silver),
        }

        for table_fqn, df_gold in gold_tables.items():
            self._write_delta(df_gold, table_fqn)
            self._row_counts[table_fqn] = self._verify_table(table_fqn)

        return gold_tables

    # ==================================================================
    # Orchestration
    # ==================================================================

    def run(self) -> Dict[str, int]:
        """Execute the full medallion pipeline end-to-end.

        Returns
        -------
        dict[str, int]
            Row counts for every table written (keyed by FQN).
        """
        logger.info("Pipeline: starting medallion pipeline")

        # Step 0 — Catalog & schemas
        self.setup_catalog_and_schemas()

        # Step 1 — Bronze
        self.ingest_bronze()

        # Step 2 — Silver
        self.transform_silver()

        # Step 3 — Gold
        self.build_gold_layer()

        # Step 4 — Summary
        self.print_summary()

        logger.info("Pipeline: completed successfully")
        return self._row_counts

    def print_summary(self) -> None:
        """Print a formatted summary of all tables and row counts."""
        separator = "=" * 60
        print(separator)
        print("     Medallion Architecture — Pipeline Summary")
        print(separator)

        display_names = {
            self.cfg.bronze_fqn: f"Bronze — {self.cfg.bronze_table}",
            self.cfg.silver_fqn: f"Silver — {self.cfg.silver_table}",
            self.cfg.gold_condition_fqn: f"Gold   — {self.cfg.gold_condition_table}",
            self.cfg.gold_admission_fqn: f"Gold   — {self.cfg.gold_admission_table}",
            self.cfg.gold_hospital_fqn: f"Gold   — {self.cfg.gold_hospital_table}",
        }

        for fqn, label in display_names.items():
            count = self._row_counts.get(fqn, 0)
            print(f"  {label:40s} | {count:>8,} rows")

        print(separator)
        print("\u2705 Medallion pipeline completed successfully!")
