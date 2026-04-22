# Databricks notebook source
# ADLS Gen2 ETL Pipeline
#
# Author: Pavan Kumar Reddy Duddekuntla
# Role: Senior Data Engineer
# Experience: 10+ years in Big Data & Cloud Data Engineering
# Expertise: Azure Data Factory, Databricks, Spark, ADLS Gen2, Delta Lake, ETL/ELT Pipelines
#
# Overview:
#   Config-driven, OOP-based pipeline for reading and writing files
#   from Azure Data Lake Storage Gen2.
#   All connection details, OAuth properties, and file paths are
#   externalized to `etl_config.json` — zero hardcoded values.
#
# Configuration:
#   Config file     : Azure_Databricks/etl_config.json
#   Secret scope    : adls-scope
#   Storage account : bronzeadlsstorage
#   Container       : ainput


# ======================================================================
# SECTION: ADLS Utility Functions
# ======================================================================

import json


class ADLSManager:
    """Manages ADLS Gen2 connectivity, file I/O, and ETL orchestration."""

    def __init__(self, config_path):
        """Initialize manager by loading and parsing the config file."""
        self.config_path = config_path
        self.config = self._load_config(config_path)
        self._parse_config()
        self.dataframes = {}

    # ------------------------------------------------------------------ #
    #  Private helpers                                                     #
    # ------------------------------------------------------------------ #

    def _load_config(self, path):
        """Load ETL configuration from a JSON file."""
        with open(path, "r") as f:
            cfg = json.load(f)
        print(f"\u2714 Config loaded from: {path}")
        return cfg

    def _parse_config(self):
        """Extract and store config sections as instance attributes."""
        self.adls_cfg   = self.config["adls_connection"]
        self.oauth_cfg  = self.config["oauth_config"]
        self.file_cfg   = self.config["file_paths"]
        self.output_cfg = self.config["output"]

        self.storage_account = self.adls_cfg["storage_account"]
        self.container       = self.adls_cfg["container"]
        self.client_id       = self.adls_cfg["client_id"]
        self.tenant_id       = self.adls_cfg["tenant_id"]

    # ------------------------------------------------------------------ #
    #  Connection                                                          #
    # ------------------------------------------------------------------ #

    def configure_oauth(self):
        """Set up OAuth authentication using config-driven Spark properties."""
        client_secret = dbutils.secrets.get(
            scope=self.adls_cfg["secret_scope"],
            key=self.adls_cfg["secret_key"]
        )
        base     = f"{self.storage_account}.dfs.core.windows.net"
        endpoint = self.oauth_cfg["endpoint_template"].format(tenant_id=self.tenant_id)

        placeholders = {            "base": base,
            "auth_type": self.oauth_cfg["auth_type"],
            "provider_type": self.oauth_cfg["provider_type"],
            "client_id": self.client_id,
            "client_secret": client_secret,
            "endpoint": endpoint,
        }

        {
            "base": base,
            "auth_type": self.oauth_cfg["auth_type"],
            "provider_type": self.oauth_cfg["provider_type"],
            "client_id": self.client_id,
            "client_secret": client_secret,
            "endpoint": endpoint,
        }

        for key_tmpl, val_tmpl in self.oauth_cfg["spark_properties"].items():
            spark.conf.set(
                key_tmpl.format(**placeholders),
                val_tmpl.format(**placeholders)
            )

        prop_count = len(self.oauth_cfg["spark_properties"])
        print(f"\u2714 OAuth configured for: {self.storage_account} ({prop_count} properties set)")
        return self

    # ------------------------------------------------------------------ #
    #  Path builder                                                        #
    # ------------------------------------------------------------------ #

    def get_path(self, path=""):
        """Build an ABFSS URI for ADLS Gen2."""
        base = f"abfss://{self.container}@{self.storage_account}.dfs.core.windows.net"
        return f"{base}/{path}" if path else f"{base}/"

    # ------------------------------------------------------------------ #
    #  File operations                                                     #
    # ------------------------------------------------------------------ #

    def list_files(self, path=""):
        """List files/folders at an ADLS Gen2 path."""
        return dbutils.fs.ls(self.get_path(path))

    def read_file(self, file_path, file_format="csv", **options):
        """Read a single file from ADLS Gen2 into a DataFrame."""
        full_path = self.get_path(file_path)
        reader = spark.read.format(file_format)
        for k, v in options.items():
            reader = reader.option(k, v)
        df = reader.load(full_path)
        print(f"\u2714 Read {file_format.upper()}: {file_path}")
        return df

    def read_all_sources(self):
        """Read every source declared in file_paths config into self.dataframes."""
        for name, path in self.file_cfg.items():
            opts = {"header": "true"} if name == "csv" else {}
            self.dataframes[name] = self.read_file(path, name, **opts)
        return self

    def write_file(self, df, output_path=None, file_format=None, mode=None):
        """Write a DataFrame to ADLS Gen2 (defaults from output config)."""
        output_path = output_path or self.output_cfg["path"]
        file_format = file_format or self.output_cfg["format"]
        mode        = mode or self.output_cfg["mode"]
        full_path   = self.get_path(output_path)
        df.write.format(file_format).mode(mode).save(full_path)
        print(f"\u2714 Data written to: {full_path}")
        return self

    # ------------------------------------------------------------------ #
    #  Display helpers                                                     #
    # ------------------------------------------------------------------ #

    def display_files(self, path=""):
        """Display file listing for a given ADLS path."""
        display(self.list_files(path))

    def display_dataframe(self, name):
        """Display a loaded DataFrame by its config key name."""
        if name in self.dataframes:
            print(f"--- {name.upper()} DataFrame ---")
            display(self.dataframes[name])
        else:
            print(f"\u26a0 DataFrame '{name}' not found. Available: {list(self.dataframes.keys())}")

    def print_summary(self):
        """Print a full summary of the loaded configuration."""
        print("=" * 45)
        print("         ETL Config Summary")
        print("=" * 45)
        print(f"Storage Account  : {self.storage_account}")
        print(f"Container        : {self.container}")
        print(f"Client ID        : {self.client_id}")
        print(f"Tenant ID        : {self.tenant_id}")
        print(f"Secret Scope     : {self.adls_cfg['secret_scope']}")
        print(f"Secret Key       : {self.adls_cfg['secret_key']}")
        print(f"Auth Type        : {self.oauth_cfg['auth_type']}")
        print(f"Provider Type    : {self.oauth_cfg['provider_type']}")
        print(f"Spark Properties : {len(self.oauth_cfg['spark_properties'])} configured")
        print(f"DataFrames       : {list(self.dataframes.keys())}")
        print(f"Output Path      : {self.output_cfg['path']}")
        print(f"Output Format    : {self.output_cfg['format']}")
        print(f"Output Mode      : {self.output_cfg['mode']}")
        print("=" * 45)

    # ------------------------------------------------------------------ #
    #  Pipeline orchestrator                                               #
    # ------------------------------------------------------------------ #

    def run_pipeline(self):
        """Main entry: configure -> list -> read all -> write -> summarize."""
        print("\U0001f680 Starting ETL Pipeline...\n")

        # Step 1: Authenticate
        self.configure_oauth()

        # Step 2: List available files
        print("\n--- Listing Files ---")
        self.display_files()
        self.display_files("ainput1")

        # Step 3: Read all configured sources
        print("\n--- Reading Data Sources ---")
        self.read_all_sources()
        for name in self.dataframes:
            self.display_dataframe(name)

        # Step 4: Write output
        print("\n--- Writing Output ---")
        self.write_file(self.dataframes["parquet"])

        # Step 5: Summary
        print()
        self.print_summary()

        print("\n\u2705 Pipeline completed successfully!")
        return self
