# Databricks notebook source
# DBTITLE 1,Gold Layer Title
# MAGIC %md
# MAGIC # Gold Layer: World Bank Aggregate
# MAGIC
# MAGIC Reads clean population data from the Silver Managed Delta table (`default.worldbank_population_silver`) and creates business-ready Gold aggregation tables:
# MAGIC
# MAGIC 1. **Country Stats** (`default.worldbank_country_stats`) — per-country summary statistics (latest population, historical average, CAGR, year-over-year growth)
# MAGIC 2. **Regional Rankings** (`default.worldbank_regional_rankings`) — top countries by population within each World Bank region, using window functions
# MAGIC
# MAGIC ## Key Changes from the Spotify Gold Layer
# MAGIC
# MAGIC | Concern | Spotify (old) | World Bank (new) |
# MAGIC | --- | --- | --- |
# MAGIC | Source | External Delta at `/tmp/silver/spotify_clean` (DBFS) | **Managed Delta** `default.worldbank_population_silver` |
# MAGIC | Aggregation 1 | Artist stats (track count, peak popularity, avg danceability) | Country stats (latest population, avg population, CAGR, YoY growth) |
# MAGIC | Aggregation 2 | Track ranking per artist (window function on popularity) | Regional population rankings (window function partitioned by region) |
# MAGIC | Optimization | Z-Order on `artist_id` | Z-Order on `country_iso3` |
# MAGIC | Storage | External Delta at `/tmp/gold/` | **Managed Delta** via `saveAsTable` |

# COMMAND ----------

# DBTITLE 1,WorldBankAggregator Class
from pyspark.sql.functions import col, avg, count, max, min, rank, desc, round, lag, when, isnull, lit, pow
from pyspark.sql.window import Window
from delta.tables import DeltaTable


class WorldBankAggregator:
    """
    Duty: Create business-ready Gold aggregation tables from the Silver
    population data.

    Produces two Managed Delta tables:
      1. Country Stats  — per-country summary metrics (latest population,
         historical average, compound annual growth rate, YoY change)
      2. Regional Rankings — top-N countries by population within each
         World Bank region, using Spark window functions

    Both tables are written via saveAsTable (Managed Delta, no DBFS paths).
    """

    def __init__(self, silver_table: str = "default.worldbank_population_silver"):
        """
        Initialize the aggregator with the Silver source table name.

        Args:
            silver_table: Fully qualified name of the Silver Managed Delta table.
        """
        self.silver_table = silver_table

    def read_silver(self):
        """
        Read the Silver Managed Delta table into a Spark DataFrame.

        Uses spark.table() since the table is Managed (no DBFS path).

        Returns:
            Spark DataFrame with clean population records (one row per country-year).
        """
        # dropDuplicates ensures one row per country-year even if the Silver table
        # has duplicate rows from partial pipeline runs.
        return spark.table(self.silver_table).dropDuplicates(["country_iso3", "year"])

    def compute_country_stats(self):
        """
        Aggregate per-country population statistics.

        Computes for each country:
          - latest_year:        most recent year with data
          - latest_population:  population in the latest year
          - avg_population:     historical mean population
          - min_population:     historical minimum
          - max_population:     historical maximum
          - cagr:               compound annual growth rate over the full period
          - yoy_growth_pct:     year-over-year percentage change for the latest year

        Uses standard groupBy aggregations plus lag() window function
        for the YoY growth calculation.

        Returns:
            Spark DataFrame with one row per country.
        """
        silver_df = self.read_silver()

        # Cast year to int for calculations
        df = silver_df.withColumn("year_int", col("year").cast("int"))

        # Basic aggregates per country
        basic_stats = df.groupBy("country_iso3", "country_name").agg(
            max("year_int").alias("latest_year"),
            min("year_int").alias("earliest_year"),
            round(avg("population"), 0).alias("avg_population"),
            min("population").alias("min_population"),
            max("population").alias("max_population"),
        )

        # Latest year population per country (window function: rank=1 by year desc)
        latest_window = Window.partitionBy("country_iso3").orderBy(desc("year_int"))
        latest_pop = df.withColumn("rn", rank().over(latest_window)) \
            .filter(col("rn") == 1) \
            .select("country_iso3", col("population").alias("latest_population"))

        # Earliest year population per country (for CAGR calculation)
        earliest_window = Window.partitionBy("country_iso3").orderBy("year_int")
        earliest_pop = df.withColumn("rn", rank().over(earliest_window)) \
            .filter(col("rn") == 1) \
            .select("country_iso3", col("population").alias("earliest_population"))

        # YoY growth for the latest year using lag() window function
        yoy_window = Window.partitionBy("country_iso3").orderBy("year_int")
        yoy_df = df.withColumn("prev_pop", lag("population").over(yoy_window)) \
            .withColumn("yoy_growth_pct",
                when(col("prev_pop").isNotNull() & (col("prev_pop") != 0),
                    round((col("population") - col("prev_pop")) / col("prev_pop") * 100, 2))) \
            .withColumn("rn", rank().over(latest_window)) \
            .filter(col("rn") == 1) \
            .select("country_iso3", "yoy_growth_pct")

        # Join all stats and compute CAGR
        # CAGR = (latest_pop / earliest_pop)^(1 / num_years) - 1
        country_stats = basic_stats \
            .join(latest_pop, "country_iso3") \
            .join(earliest_pop, "country_iso3") \
            .join(yoy_df, "country_iso3") \
            .withColumn("cagr",
                when((col("earliest_population") > 0) & (col("latest_year") > col("earliest_year")),
                    round(pow(col("latest_population") / col("earliest_population"),
                              1.0 / (col("latest_year") - col("earliest_year"))) - 1, 4))) \
            .select("country_iso3", "country_name", "latest_year", "latest_population",
                    "avg_population", "min_population", "max_population", "cagr", "yoy_growth_pct") \
            .orderBy(desc("latest_population"))

        return country_stats

    def compute_regional_rankings(self, top_n: int = 10):
        """
        Rank countries by population within each World Bank region.

        Uses a window function partitioned by region and ordered by
        population descending, then filters to the top-N per region.

        Args:
            top_n: Number of top countries to keep per region.

        Returns:
            Spark DataFrame with regional population rankings.
        """
        import requests

        silver_df = self.read_silver()

        # Fetch country-to-region mapping from the World Bank API (no auth needed)
        resp = requests.get("https://api.worldbank.org/v2/country",
                            params={"format": "json", "per_page": 1000})
        _, countries = resp.json()

        # Build mapping — skip aggregates (region.id == "NA") and entries without iso3
        region_rows = []
        for c in countries:
            region = c.get("region", {})
            iso3 = c.get("id")
            if iso3 and region.get("id") and region.get("id") != "NA":
                region_rows.append((iso3, region.get("value")))

        # Use explicit schema to avoid type inference issues with spark.createDataFrame
        from pyspark.sql.types import StructType, StructField, StringType
        region_schema = StructType([
            StructField("country_iso3", StringType(), True),
            StructField("region", StringType(), True),
        ])
        region_df = spark.createDataFrame(region_rows, region_schema)

        # Get latest year population per country
        latest_year = silver_df.select(max(col("year").cast("int"))).collect()[0][0]
        latest_pop = silver_df.filter(col("year") == lit(str(latest_year))) \
            .select("country_iso3", "country_name", "population") \
            .join(region_df, "country_iso3", "inner")

        # Rank countries within each region by population descending
        window_spec = Window.partitionBy("region").orderBy(desc("population"))
        rankings = latest_pop.withColumn("rank", rank().over(window_spec)) \
            .filter(col("rank") <= top_n) \
            .select("region", "rank", "country_iso3", "country_name", "population") \
            .orderBy("region", "rank")

        return rankings

    def write_gold_tables(self, country_stats_df, regional_rankings_df):
        """
        Write both Gold aggregation tables as Managed Delta tables.

        Tables written:
          - default.worldbank_country_stats       (overwrite mode)
          - default.worldbank_regional_rankings   (overwrite mode)

        Uses saveAsTable with format('delta') — no DBFS paths.
        """
        country_stats_df.write \
            .format("delta") \
            .mode("overwrite") \
            .saveAsTable("default.worldbank_country_stats")
        print("Created Gold Table: Country Stats")

        regional_rankings_df.write \
            .format("delta") \
            .mode("overwrite") \
            .saveAsTable("default.worldbank_regional_rankings")
        print("Created Gold Table: Regional Rankings")

    def optimize_gold(self, table_name: str, zorder_column: str = "country_iso3"):
        """
        Compact files and Z-Order the Gold table for query performance.

        Uses DeltaTable.forName() (not forPath) to connect to the Managed
        table, then calls optimize().executeZOrderBy().

        Args:
            table_name: Fully qualified name of the Gold table to optimize.
            zorder_column: Column to Z-Order by (default: country_iso3).
        """
        print(f"Optimizing and Z-Ordering '{table_name}' by '{zorder_column}'...")
        gold_table = DeltaTable.forName(spark, table_name)
        gold_table.optimize().executeZOrderBy(zorder_column)
        print("Optimization complete.")

# COMMAND ----------

# DBTITLE 1,Orchestration Skeleton
# ============================================================
# ORCHESTRATION: Silver → Gold
# ============================================================
# This cell ties the WorldBankAggregator methods together:
#   1. Read clean data from the Silver Managed Delta table
#   2. Compute per-country statistics (latest pop, avg, CAGR, YoY growth)
#   3. Compute regional population rankings (window function)
#   4. Write both Gold tables as Managed Delta
#   5. Optimize (Z-Order) the country stats table

aggregator = WorldBankAggregator(
    silver_table="default.worldbank_population_silver",
)
silver_df = aggregator.read_silver()
country_stats = aggregator.compute_country_stats()
regional_rankings = aggregator.compute_regional_rankings(top_n=10)
aggregator.write_gold_tables(country_stats, regional_rankings)
aggregator.optimize_gold("default.worldbank_country_stats", "country_iso3")
print("Gold layer aggregation complete.")

display(country_stats)