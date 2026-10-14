# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# DBTITLE 1,Bronze Layer Title
# MAGIC %md
# MAGIC # Bronze Layer: World Bank Multi-Indicator Extract
# MAGIC
# MAGIC Extracts **human activity & spatial trends** data for every country from the **World Bank API** across three indicators and writes them to a **Managed Delta table** (`default.worldbank_indicators_bronze`) for downstream transformation.
# MAGIC
# MAGIC ## Indicators
# MAGIC
# MAGIC | Code | Name | Story |
# MAGIC | --- | --- | --- |
# MAGIC | `EN.POP.DNST` | Population Density (people per sq. km) | Tracks how crowded landmasses are becoming |
# MAGIC | `SP.URB.TOTL.IN.ZS` | Urban Population (% of total) | Tracks mass migration from rural areas into cities |
# MAGIC | `EG.ELC.ACCS.ZS` | Access to Electricity (% of population) | Maps the spread of basic modern infrastructure |
# MAGIC
# MAGIC ## Key Differences from the Previous Pipeline
# MAGIC
# MAGIC | Concern | Old (Population Only) | New (Multi-Indicator) |
# MAGIC | --- | --- | --- |
# MAGIC | Indicators | Single: `SP.POP.TOTL` | **Three**: density, urban %, electricity % |
# MAGIC | Pagination | Metadata-driven (page 1..N) | Same — looped per indicator |
# MAGIC | Storage | `default.worldbank_population_bronze` | `default.worldbank_indicators_bronze` |
# MAGIC | Write mode | Append (accumulates duplicates) | **Overwrite** (fresh each run) |

# COMMAND ----------

# DBTITLE 1,Environment Check
print("Databricks Serverless is ready!")

# Confirm the Pandas memory bridge is available (required for Serverless-safe ingestion)
import pandas as pd
print(f"pandas version: {pd.__version__}")
print(f"Spark session active: {spark is not None}")

# COMMAND ----------

# DBTITLE 1,No Authentication Needed
# --- No Authentication Needed ---
# Unlike Spotify, the World Bank API is completely free and requires
# NO authentication keys, tokens, or secret scopes.
#
# We simply send HTTP GET requests to the public endpoint:
#   https://api.worldbank.org/v2/country/all/indicator/SP.POP.TOTL?format=json
#
# This cell replaces the old Spotify secret-scope setup entirely.

import requests

BASE_URL = "https://api.worldbank.org/v2"
INDICATORS = [
    "EN.POP.DNST",        # Population Density (people per sq. km)
    "SP.URB.TOTL.IN.ZS",  # Urban Population (% of total)
    "EG.ELC.ACCS.ZS",     # Access to Electricity (% of population)
]

# Quick connectivity check — fetch page 1 of the first indicator to confirm the API is reachable
test_url = f"{BASE_URL}/country/all/indicator/{INDICATORS[0]}"
test_params = {"format": "json", "per_page": 1, "page": 1}
test_resp = requests.get(test_url, params=test_params)
print(f"World Bank API connectivity: HTTP {test_resp.status_code}")
if test_resp.status_code == 200:
    meta, _ = test_resp.json()
    print(f"Total records for {INDICATORS[0]}: {meta['total']:,}")
    print(f"Total pages at per_page=1: {meta['pages']}")

# COMMAND ----------

# DBTITLE 1,WorldBankAPIClient Class
import requests
import time


class WorldBankAPIClient:
    """
    Duty: Fetch multi-indicator data for every country from the World Bank API.

    The World Bank API is completely free and requires NO authentication.
    Uses **metadata-driven pagination**: the response metadata tells you the
    total number of pages, and you loop through them using page numbers.

    The client extracts multiple indicators in a single pass by looping
    through each indicator code and paginating through all of its pages.

    Endpoint:
        https://api.worldbank.org/v2/country/all/indicator/{indicator_code}?format=json

    Response structure:
        [0] = metadata dict  →  {'page': 1, 'pages': 6, 'per_page': 1000, 'total': 15917}
        [1] = list of records (one per country-year)
    """

    BASE_URL = "https://api.worldbank.org/v2"
    INDICATORS = [
        "EN.POP.DNST",        # Population Density (people per sq. km)
        "SP.URB.TOTL.IN.ZS",  # Urban Population (% of total)
        "EG.ELC.ACCS.ZS",     # Access to Electricity (% of population)
    ]

    def __init__(self, per_page: int = 1000):
        """
        Initialize the API client.

        Args:
            per_page: Number of records to request per page (max 1000).
                      A larger value means fewer HTTP round-trips.
        """
        self.per_page = per_page

    def fetch_page(self, indicator: str, page_number: int) -> tuple:
        """
        Fetch a single page of indicator data from the World Bank API.

        Args:
            indicator: The World Bank indicator code (e.g. 'EN.POP.DNST').
            page_number: The 1-indexed page number to retrieve.

        Returns:
            A tuple (metadata_dict, list_of_records) where:
              - metadata_dict contains 'page', 'pages', 'per_page', 'total'
              - list_of_records is the array of country-year indicator entries

        Raises:
            RuntimeError: If the API returns a non-200 status code after retries.
        """
        url = f"{self.BASE_URL}/country/all/indicator/{indicator}"
        params = {"format": "json", "per_page": self.per_page, "page": page_number}

        max_retries = 3
        for attempt in range(max_retries):
            response = requests.get(url, params=params, timeout=10)

            # Handle rate limiting (HTTP 429) — rare for the World Bank API
            if response.status_code == 429:
                retry_after = int(response.headers.get("Retry-After", 5))
                print(f"Rate limited on page {page_number}! Pausing {retry_after}s... (attempt {attempt + 1}/{max_retries})")
                time.sleep(retry_after)
                continue

            if response.status_code != 200:
                raise RuntimeError(f"API returned HTTP {response.status_code} on page {page_number}: {response.text}")

            # World Bank API returns [metadata_dict, list_of_records]
            data = response.json()
            metadata = data[0]
            records = data[1] if len(data) > 1 else []
            return (metadata, records)

        raise RuntimeError(f"Max retries ({max_retries}) hit on page {page_number}. Aborting.")

    def get_total_pages(self, indicator: str) -> int:
        """
        Query the first page to discover how many total pages exist for an indicator.

        Uses the 'pages' field in the API response metadata to determine
        the total page count without downloading every page.

        Args:
            indicator: The World Bank indicator code.

        Returns:
            Total number of pages (int).
        """
        metadata, _ = self.fetch_page(indicator, 1)
        return metadata["pages"]

    def extract_all_pages(self, indicators: list = None) -> list:
        """
        Loop through all indicators and all pages using metadata-driven pagination.

        For each indicator, fetches the total page count from metadata, then
        iterates through pages 1..N to collect all records.

        Args:
            indicators: List of World Bank indicator codes. Defaults to self.INDICATORS.

        Returns:
            A single aggregated list of all indicator records across all pages.
        """
        if indicators is None:
            indicators = self.INDICATORS

        all_records = []
        for indicator in indicators:
            total_pages = self.get_total_pages(indicator)
            print(f"\n--- Indicator: {indicator} ({total_pages} pages, per_page={self.per_page}) ---")

            for page_num in range(1, total_pages + 1):
                _, records = self.fetch_page(indicator, page_num)
                all_records.extend(records)
                print(f"  Page {page_num}/{total_pages} — {len(records)} records (running total: {len(all_records):,})")

        print(f"\nExtraction complete: {len(all_records):,} total records across {len(indicators)} indicators.")
        return all_records

# COMMAND ----------

# DBTITLE 1,WorldBankDataLoader Class
from pyspark.sql.functions import current_timestamp, col
import pandas as pd


class WorldBankDataLoader:
    """
    Duty: Load raw World Bank population records into a Managed Delta table
    using the Pandas memory bridge (Serverless-compliant).

    The Pandas memory bridge avoids DBFS entirely — raw API records are
    converted to a pandas DataFrame, then to a Spark DataFrame via
    spark.createDataFrame(), and written directly to a Managed Delta table
    via saveAsTable(). This is the recommended pattern on Serverless compute
    where DBFS file paths are not available.
    """

    def __init__(self, table_name: str = "default.worldbank_indicators_bronze"):
        """
        Initialize the loader with the target Managed Delta table name.

        Args:
            table_name: Fully qualified Managed Delta table name.
                        Defaults to 'default.worldbank_population_bronze'.
        """
        self.table_name = table_name

    def to_pandas_df(self, records: list) -> pd.DataFrame:
        """
        Convert raw API records (list of dicts) into a pandas DataFrame.

        The World Bank API returns flat records with fields like:
          - country.iso3Code  →  country_iso3
          - country.name      →  country_name
          - country.id        →  country_id
          - indicator.id      →  indicator_code
          - indicator.name    →  indicator_name
          - date (year)       →  year
          - value (indicator value) → value
          - unit              →  unit
          - obs_status        →  obs_status
          - decimal           →  decimal

        This method flattens nested dicts and renames fields to snake_case
        for downstream Spark processing.

        Returns:
            A pandas DataFrame with one row per country-year.
        """
        flattened = []
        for record in records:
            country = record.get("country", {})
            indicator = record.get("indicator", {})
            flattened.append({
                "country_iso3":   record.get("countryiso3code"),
                "country_name":   country.get("value"),
                "country_id":     country.get("id"),
                "indicator_code": indicator.get("id"),
                "indicator_name": indicator.get("value"),
                "year":           record.get("date"),
                "value":           record.get("value"),
                "unit":           record.get("unit"),
                "obs_status":     record.get("obs_status"),
                "decimal":        record.get("decimal"),
            })

        pdf = pd.DataFrame(flattened)
        print(f"Flattened {len(pdf):,} records into a pandas DataFrame (shape: {pdf.shape})")
        return pdf

    def write_to_delta(self, pdf: pd.DataFrame, mode: str = "append") -> None:
        """
        Use the Pandas memory bridge to write to a Managed Delta table.

        Steps:
          1. Convert pandas DataFrame → Spark DataFrame (spark.createDataFrame)
          2. Add an ingestion_timestamp metadata column
          3. Write to the Managed Delta table via saveAsTable (no DBFS path)

        Args:
            pdf: The pandas DataFrame containing flattened indicator records.
            mode: Write mode — 'append' or 'overwrite'.
        """
        # 1. Convert pandas DataFrame → Spark DataFrame (Pandas memory bridge)
        #    Apply explicit casts — pandas infers float64 for population (due to
        #    None/NaN handling) and int64 for decimal; we want LongType and
        #    IntegerType respectively so the Bronze schema matches Silver's.
        spark_df = spark.createDataFrame(pdf) \
            .withColumn("value", col("value").cast("double")) \
            .withColumn("decimal", col("decimal").cast("int"))

        # 2. Add the ingestion timestamp metadata column
        spark_df = spark_df.withColumn("ingestion_timestamp", current_timestamp())

        # 3. Write to the Managed Delta table via saveAsTable (no DBFS path needed)
        writer = spark_df.write \
            .mode(mode) \
            .format("delta")

        # When overwriting, allow schema replacement (fixes type changes from re-runs)
        if mode == "overwrite":
            writer = writer.option("overwriteSchema", "true")

        writer.saveAsTable(self.table_name)

        print(f"Successfully wrote {spark_df.count()} records to Managed Delta table: {self.table_name}")

# COMMAND ----------

# DBTITLE 1,Orchestration Skeleton
# ============================================================
# ORCHESTRATION: Extract → Load
# ============================================================
# This cell ties the two classes together:
#   1. WorldBankAPIClient extracts all 3 indicators via metadata-driven pagination
#   2. WorldBankDataLoader converts them to pandas → Spark → Managed Delta

client = WorldBankAPIClient(per_page=1000)
raw_records = client.extract_all_pages()
print(f"Extracted {len(raw_records):,} indicator records across {len(client.INDICATORS)} indicators")

loader = WorldBankDataLoader(table_name="default.worldbank_indicators_bronze")
pdf = loader.to_pandas_df(raw_records)
loader.write_to_delta(pdf, mode="overwrite")
print("Bronze layer ingestion complete.")

# COMMAND ----------

# DBTITLE 1,Preview Bronze Table
display(spark.table("default.worldbank_indicators_bronze").limit(20))