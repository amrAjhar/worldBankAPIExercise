# Databricks notebook source
# DBTITLE 1,Silver Layer Title
# MAGIC %md
# MAGIC # Silver Layer: World Bank Multi-Indicator Transform
# MAGIC
# MAGIC Reads raw multi-indicator data from the Bronze Managed Delta table (`default.worldbank_indicators_bronze`), flattens nested structs, applies a strict schema with proper data types, and writes clean tabular data to a Silver Managed Delta table (`default.worldbank_indicators_silver`) using `saveAsTable`.
# MAGIC
# MAGIC ## Schema
# MAGIC
# MAGIC | Column | Type | Notes |
# MAGIC | --- | --- | --- |
# MAGIC | country_iso3 | string | PK part 1 |
# MAGIC | country_name | string | |
# MAGIC | country_id | string | 2-letter code from API |
# MAGIC | indicator_code | string | PK part 3 (e.g. `EN.POP.DNST`) |
# MAGIC | indicator_name | string | |
# MAGIC | year | string | PK part 2 (stored as string from API) |
# MAGIC | value | double | Indicator value (density, %, etc.) |
# MAGIC | unit | string | |
# MAGIC | obs_status | string | |
# MAGIC | decimal | int | |
# MAGIC | ingestion_timestamp | timestamp | |
# MAGIC
# MAGIC ## Merge key
# MAGIC
# MAGIC `(country_iso3, year, indicator_code)` — one record per country per year per indicator.

# COMMAND ----------

# DBTITLE 1,WorldBankTransformer Class
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, LongType, DoubleType, TimestampType
)
from pyspark.sql.functions import col, current_timestamp
from delta.tables import DeltaTable


class WorldBankTransformer:
    """
    Duty: Transform raw World Bank population records from the Bronze layer
    into a clean, strongly-typed Silver table.

    This class encapsulates the full Silver-layer pipeline:
      1. Read the Bronze Managed Delta table (no DBFS path needed)
      2. Flatten nested country/indicator structs and select relevant columns
      3. Apply a strict schema with correct data types (cast, drop nulls)
      4. UPSERT into the Silver Managed Delta table via Delta MERGE
      5. Support schema evolution (add new columns without breaking MERGE)
    """

    # Strict schema for the Silver layer — one row per country-year
    SILVER_SCHEMA = StructType([
        StructField("country_iso3",    StringType(),  False),  # PK part 1
        StructField("country_name",    StringType(),  True),
        StructField("country_id",      StringType(),  True),
        StructField("indicator_code",  StringType(),  False),  # PK part 3
        StructField("indicator_name",  StringType(),  True),
        StructField("year",            StringType(),  False),  # PK part 2 (stored as string from API)
        StructField("value",           DoubleType(),   True),   # indicator value (density, %, etc.)
        StructField("unit",             StringType(),  True),
        StructField("obs_status",       StringType(),  True),
        StructField("decimal",          IntegerType(), True),
        StructField("ingestion_timestamp", TimestampType(), True),
    ])

    # Composite merge key — one record per country per year
    MERGE_KEY = "target.country_iso3 = updates.country_iso3 AND target.year = updates.year AND target.indicator_code = updates.indicator_code"

    def __init__(
        self,
        bronze_table: str = "default.worldbank_indicators_bronze",
        silver_table: str = "default.worldbank_indicators_silver",
    ):
        """
        Initialize the transformer with Bronze (source) and Silver (target)
        Managed Delta table names.

        Args:
            bronze_table: Fully qualified name of the Bronze Managed Delta table.
            silver_table: Fully qualified name of the Silver Managed Delta table.
        """
        self.bronze_table = bronze_table
        self.silver_table = silver_table

    def read_bronze(self):
        """
        Read the Bronze Managed Delta table into a Spark DataFrame.

        Uses spark.table() (not spark.read.load(path)) since the table
        is Managed — no DBFS path required.

        Returns:
            Spark DataFrame containing the raw Bronze population records.
        """
        return spark.table(self.bronze_table)

    def flatten_and_clean(self, bronze_df):
        """
        Flatten nested structs and select the columns needed for Silver.

        The Bronze table stores the World Bank API response fields, which
        include nested 'country' and 'indicator' structs. This method
        extracts:
          - country_iso3  from  country.iso3Code
          - country_name  from  country.value (or country.name)
          - country_id    from  country.id
          - indicator_code from indicator.id
          - indicator_name from indicator.value (or indicator.name)
          - year          from  date
          - population    from  value
          - unit, obs_status, decimal  (pass-through)

        Also deduplicates on (country_iso3, year) to handle API overlaps.

        Returns:
            A flattened Spark DataFrame with one row per country-year.
        """
        # The Bronze table was written via the Pandas memory bridge (notebook 02),
        # so country/indicator fields are already flattened to top-level columns.
        # Select the relevant columns and deduplicate on (country_iso3, year).
        flattened = bronze_df.select(
            col("country_iso3"),
            col("country_name"),
            col("country_id"),
            col("indicator_code"),
            col("indicator_name"),
            col("year"),
            col("value"),
            col("unit"),
            col("obs_status"),
            col("decimal"),
            col("ingestion_timestamp"),
        ).dropDuplicates(["country_iso3", "year", "indicator_code"])

        print(f"Flattened and deduplicated: {flattened.count():,} unique country-year-indicator rows")
        return flattened

    def apply_schema(self, df):
        """
        Cast the flattened DataFrame columns to match SILVER_SCHEMA.

        Ensures 'year' is usable (kept as string to match API output),
        'value' is DoubleType, and drops rows where critical PK
        fields (country_iso3, year, indicator_code) are null after casting.

        Returns:
            A Spark DataFrame conforming to SILVER_SCHEMA.
        """
        # Cast each column to match SILVER_SCHEMA
        for field in self.SILVER_SCHEMA.fields:
            df = df.withColumn(field.name, col(field.name).cast(field.dataType))

        # Drop rows where critical PK fields (country_iso3, year) are null after casting
        df = df.dropna(subset=["country_iso3", "year", "indicator_code"])

        # Also drop rows where country_iso3 is an empty string (income-level aggregates
        # like "High income" have countryiso3code="" from the API — not null, so dropna misses them)
        df = df.filter(col("country_iso3") != "")

        print(f"After schema enforcement: {df.count():,} rows with valid PKs (country_iso3, year, indicator_code)")
        return df

    def upsert_to_silver(self, df):
        """
        UPSERT the transformed DataFrame into the Silver Managed Delta table.

        Uses DeltaTable.forName(spark, table_name) to connect to the
        Managed table (not DeltaTable.forPath), then performs a MERGE
        on the composite key (country_iso3, year).

        If the table does not yet exist, falls back to a CREATE TABLE
        via saveAsTable(mode='overwrite').
        """
        # Check if the Silver Managed Delta table already exists
        if spark.catalog.tableExists(self.silver_table):
            print(f"Silver table '{self.silver_table}' exists — performing UPSERT (MERGE INTO)...")
            silver_table = DeltaTable.forName(spark, self.silver_table)

            # Build update set only for columns that exist in BOTH source and target.
            # This handles schema evolution gracefully — if the target table has
            # extra columns (e.g. from evolve_schema), the MERGE won't fail.
            target_cols = set(spark.table(self.silver_table).columns)
            source_cols = set(df.columns)
            update_set = {c: f"updates.{c}" for c in source_cols if c in target_cols}

            # MERGE on composite key (country_iso3, year)
            silver_table.alias("target") \
                .merge(df.alias("updates"), self.MERGE_KEY) \
                .whenMatchedUpdate(set=update_set) \
                .whenNotMatchedInsert(values=update_set) \
                .execute()
            print("UPSERT complete.")
        else:
            # First run — create the Silver table
            print(f"Silver table '{self.silver_table}' does not exist — creating new table...")
            df.write.mode("overwrite").format("delta").saveAsTable(self.silver_table)
            print("Silver table created.")

    def evolve_schema(self, df, new_columns: dict):
        """
        Add new columns to the Silver table via schema evolution.

        Args:
            df: The DataFrame containing the new columns.
            new_columns: Dict of {column_name: expression} to add.

        Uses .option('mergeSchema', 'true') on append, and also enables
        spark.databricks.delta.schema.autoMerge.enabled for MERGE-based
        upserts to pick up the new columns automatically.
        """
        # 1. Add new columns to the DataFrame
        for col_name, expr in new_columns.items():
            df = df.withColumn(col_name, expr)

        # 2. STANDARD APPEND APPROACH (commented out — see note below)
        # .option("mergeSchema", "true") forces Delta Lake to alter its schema on the fly.
        # This works but creates duplicate rows on re-runs, so we use MERGE instead below.
        # To use append only, uncomment these lines:
        # df.write \
        #     .format("delta") \
        #     .mode("append") \
        #     .option("mergeSchema", "true") \
        #     .saveAsTable(self.silver_table)
        # print("Schema evolved using standard append!")

        # 3. UPSERT APPROACH — add columns via ALTER TABLE then MERGE
        # (spark.databricks.delta.schema.autoMerge.enabled is not available
        # on Serverless, so we use ALTER TABLE to add the new columns before MERGE)
        existing_cols = set(spark.table(self.silver_table).columns)
        new_col_types = {
            name: df.schema[name].dataType.simpleString()
            for name in new_columns.keys()
            if name not in existing_cols
        }
        if new_col_types:
            alter_cols = ", ".join(f"`{c}` {t}" for c, t in new_col_types.items())
            spark.sql(f"ALTER TABLE {self.silver_table} ADD COLUMNS ({alter_cols})")

        silver_table = DeltaTable.forName(spark, self.silver_table)
        silver_table.alias("target") \
            .merge(df.alias("updates"), self.MERGE_KEY) \
            .whenMatchedUpdateAll() \
            .whenNotMatchedInsertAll() \
            .execute()
        print("Schema evolved using MERGE INTO!")

# COMMAND ----------

# DBTITLE 1,Orchestration Skeleton
# ============================================================
# ORCHESTRATION: Bronze → Silver
# ============================================================
# This cell ties the WorldBankTransformer methods together:
#   1. Read raw data from the Bronze Managed Delta table
#   2. Flatten nested country/indicator structs and clean
#   3. Apply the strict Silver schema (cast types, drop nulls)
#   4. UPSERT into the Silver Managed Delta table

transformer = WorldBankTransformer(
    bronze_table="default.worldbank_indicators_bronze",
    silver_table="default.worldbank_indicators_silver",
)
bronze_df = transformer.read_bronze()
flattened_df = transformer.flatten_and_clean(bronze_df)
silver_df = transformer.apply_schema(flattened_df)
transformer.upsert_to_silver(silver_df)
print("Silver layer transform complete.")