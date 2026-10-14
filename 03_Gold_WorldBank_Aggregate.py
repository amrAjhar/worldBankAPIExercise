# Databricks notebook source
# DBTITLE 1,Gold Layer Title
# MAGIC %md
# MAGIC # Gold Layer: Human Activity & Spatial Trends
# MAGIC
# MAGIC Reads clean multi-indicator data from the Silver Managed Delta table (`default.worldbank_indicators_silver`) and creates three business-ready Gold aggregation tables:
# MAGIC
# MAGIC 1. **Urbanization Velocity** (`default.worldbank_urbanization_velocity`) — 5-year rolling avg growth rate of urban population %, ranked by fastest-urbanizing countries
# MAGIC 2. **Density Spike** (`default.worldbank_density_spike`) — absolute change in population density (people/sq. km) from 2014 to 2024, ranked by largest increase
# MAGIC 3. **Vulnerable Urbanization** (`default.worldbank_vulnerable_urbanization`) — countries where urban population is growing >1% YoY but electricity access is growing <0.5% YoY
# MAGIC
# MAGIC ## Indicators Used
# MAGIC
# MAGIC | Gold Table | Indicator | Window Function |
# MAGIC | --- | --- | --- |
# MAGIC | Urbanization Velocity | `SP.URB.TOTL.IN.ZS` (Urban %) | `lag(value, 5)` for 5-year growth rate |
# MAGIC | Density Spike | `EN.POP.DNST` (Density) | `first()` / `last()` for 2014 vs 2024 |
# MAGIC | Vulnerable Urbanization | `SP.URB.TOTL.IN.ZS` + `EG.ELC.ACCS.ZS` | Multi-indicator join with `lag()` YoY growth |

# COMMAND ----------

# DBTITLE 1,WorldBankAggregator Class
from pyspark.sql.functions import col, avg, count, max, min, rank, desc, round, lag, lead, when, isnull, lit, pow
from pyspark.sql.window import Window
from delta.tables import DeltaTable


class WorldBankAggregator:
    """
    Duty: Create business-ready Gold aggregation tables from multi-indicator
    Silver data.

    Produces three Managed Delta tables:
      1. Urbanization Velocity — 5-year rolling avg growth rate of urban
         population %, ranked by fastest-urbanizing countries.
      2. Density Spike — absolute change in population density from 2014
         to 2024, ranked by largest increase.
      3. Vulnerable Urbanization — countries where urban population is
         growing >1% YoY but electricity access growth is <0.5% YoY.

    All tables are written via saveAsTable (Managed Delta, no DBFS paths).
    """

    INDICATOR_URBAN = "SP.URB.TOTL.IN.ZS"
    INDICATOR_DENSITY = "EN.POP.DNST"
    INDICATOR_ELECTRICITY = "EG.ELC.ACCS.ZS"

    def __init__(self, silver_table: str = "default.worldbank_indicators_silver"):
        """
        Initialize the aggregator with the Silver source table name.

        Args:
            silver_table: Fully qualified name of the Silver Managed Delta table.
        """
        self.silver_table = silver_table

    def read_silver(self, indicator_code: str = None):
        """
        Read the Silver Managed Delta table, optionally filtered by indicator.

        Uses spark.table() since the table is Managed (no DBFS path).

        Args:
            indicator_code: Optional indicator code to filter by.

        Returns:
            Spark DataFrame with clean indicator records (one row per country-year-indicator).
        """
        df = spark.table(self.silver_table)
        if indicator_code:
            df = df.filter(col("indicator_code") == indicator_code)
        return df.dropDuplicates(["country_iso3", "year", "indicator_code"])

    def compute_urbanization_velocity(self):
        """
        Calculate the 5-year rolling average growth rate of urban population %
        for each country, then rank by the latest 5-year growth rate.

        Uses lag(value, 5) to compare today's urban % with 5 years ago.
        Countries with the highest 5-year growth rate are urbanizing fastest.

        Returns:
            Spark DataFrame ranked by urbanization velocity (descending).
        """
        df = self.read_silver(self.INDICATOR_URBAN)
        df = df.withColumn("year_int", col("year").cast("int"))
        df = df.withColumn("value_double", col("value").cast("double")) \
            .filter(col("value_double").isNotNull())

        # Use lag to get urban % 5 years ago
        window = Window.partitionBy("country_iso3").orderBy("year_int")
        df = df.withColumn("urban_5yr_ago", lag("value_double", 5).over(window))

        # Compute 5-year growth rate: (current - 5yr_ago) / 5yr_ago * 100
        df = df.withColumn("urban_growth_5yr_pct",
            when(col("urban_5yr_ago").isNotNull() & (col("urban_5yr_ago") != 0),
                round((col("value_double") - col("urban_5yr_ago")) / col("urban_5yr_ago") * 100, 2)))

        # Get the latest year's 5-year growth rate per country
        latest_window = Window.partitionBy("country_iso3").orderBy(desc("year_int"))
        latest = df.withColumn("rn", rank().over(latest_window)) \
            .filter(col("rn") == 1) \
            .select("country_iso3", "country_name",
                    col("year_int").alias("latest_year"),
                    col("value_double").alias("latest_urban_pct"),
                    "urban_growth_5yr_pct")

        # Rank by 5-year growth rate descending
        rank_window = Window.orderBy(desc("urban_growth_5yr_pct"))
        velocity = latest.withColumn("velocity_rank", rank().over(rank_window)) \
            .filter(col("urban_growth_5yr_pct").isNotNull()) \
            .select("velocity_rank", "country_iso3", "country_name",
                    "latest_year", "latest_urban_pct", "urban_growth_5yr_pct") \
            .orderBy("velocity_rank")

        return velocity

    def compute_density_spike(self):
        """
        Calculate the absolute change in population density from 2014 to 2024.

        Filters to years 2015 and 2025, joins them per country, and computes
        the pure numeric increase in people per square kilometer.

        Returns:
            Spark DataFrame ranked by density change (descending).
        """
        df = self.read_silver(self.INDICATOR_DENSITY)
        df = df.withColumn("year_int", col("year").cast("int"))
        df = df.withColumn("value_double", col("value").cast("double"))

        # Filter to 2014 and 2024 (2025 values are NULL in the API)
        df = df.filter(col("year_int").isin(2014, 2024))
        df = df.filter(col("value_double").isNotNull())

        # Get density_2014 and density_2024 per country
        density_2014 = df.filter(col("year_int") == 2014) \
            .select("country_iso3", "country_name", col("value_double").alias("density_2014"))
        density_2024 = df.filter(col("year_int") == 2024) \
            .select("country_iso3", col("value_double").alias("density_2024"))

        # Join and compute absolute change
        spike = density_2024.join(density_2014, "country_iso3", "inner") \
            .withColumn("density_change",
                round(col("density_2024") - col("density_2014"), 2)) \
            .filter(col("density_change").isNotNull())

        # Rank by absolute change descending
        rank_window = Window.orderBy(desc("density_change"))
        spike = spike.withColumn("density_rank", rank().over(rank_window)) \
            .select("density_rank", "country_iso3", "country_name",
                    "density_2014", "density_2024", "density_change") \
            .orderBy("density_rank")

        return spike

    def compute_vulnerable_urbanization(self):
        """
        Join urban population growth with electricity access growth to find
        countries where cities are expanding faster than infrastructure.

        Identifies "Vulnerable Urbanization" — countries where urban population
        is growing at >1% YoY but electricity access is growing at <0.5% YoY,
        indicating cities expanding faster than their infrastructure can handle.

        Returns:
            Spark DataFrame of vulnerable countries ordered by vulnerability gap.
        """
        # --- Urban population YoY growth ---
        urban_df = self.read_silver(self.INDICATOR_URBAN)
        urban_df = urban_df.withColumn("year_int", col("year").cast("int"))
        urban_df = urban_df.withColumn("value_double", col("value").cast("double")) \
            .filter(col("value_double").isNotNull())

        urban_window = Window.partitionBy("country_iso3").orderBy("year_int")
        urban_df = urban_df.withColumn("prev_urban", lag("value_double").over(urban_window)) \
            .withColumn("urban_yoy_growth",
                when(col("prev_urban").isNotNull() & (col("prev_urban") != 0),
                    round((col("value_double") - col("prev_urban")) / col("prev_urban") * 100, 2)))

        # Get latest year's urban YoY growth
        latest_window = Window.partitionBy("country_iso3").orderBy(desc("year_int"))
        urban_latest = urban_df.withColumn("rn", rank().over(latest_window)) \
            .filter(col("rn") == 1) \
            .select("country_iso3", "country_name",
                    col("year_int").alias("latest_year"),
                    col("value_double").alias("latest_urban_pct"),
                    "urban_yoy_growth")

        # --- Electricity access YoY growth ---
        elec_df = self.read_silver(self.INDICATOR_ELECTRICITY)
        elec_df = elec_df.withColumn("year_int", col("year").cast("int"))
        elec_df = elec_df.withColumn("value_double", col("value").cast("double")) \
            .filter(col("value_double").isNotNull())

        elec_window = Window.partitionBy("country_iso3").orderBy("year_int")
        elec_df = elec_df.withColumn("prev_elec", lag("value_double").over(elec_window)) \
            .withColumn("electricity_yoy_growth",
                when(col("prev_elec").isNotNull() & (col("prev_elec") != 0),
                    round((col("value_double") - col("prev_elec")) / col("prev_elec") * 100, 2)))

        elec_latest = elec_df.withColumn("rn", rank().over(latest_window)) \
            .filter(col("rn") == 1) \
            .select("country_iso3",
                    col("value_double").alias("latest_electricity_pct"),
                    "electricity_yoy_growth")

        # --- Join urban and electricity data ---
        vulnerable = urban_latest.join(elec_latest, "country_iso3", "inner") \
            .withColumn("vulnerability_gap",
                round(col("urban_yoy_growth") - col("electricity_yoy_growth"), 2)) \
            .filter(
                (col("urban_yoy_growth") > 1.0) &
                (col("electricity_yoy_growth") < 0.5) &
                col("urban_yoy_growth").isNotNull() &
                col("electricity_yoy_growth").isNotNull()
            ) \
            .select("country_iso3", "country_name", "latest_year",
                    "latest_urban_pct", "latest_electricity_pct",
                    "urban_yoy_growth", "electricity_yoy_growth", "vulnerability_gap") \
            .orderBy(desc("vulnerability_gap"))

        return vulnerable

    def write_gold_tables(self, velocity_df, density_df, vulnerable_df):
        """
        Write all three Gold aggregation tables as Managed Delta.

        Tables written:
          - default.worldbank_urbanization_velocity   (overwrite mode)
          - default.worldbank_density_spike           (overwrite mode)
          - default.worldbank_vulnerable_urbanization  (overwrite mode)

        Uses saveAsTable with format('delta') — no DBFS paths.
        """
        velocity_df.write \
            .format("delta") \
            .mode("overwrite") \
            .saveAsTable("default.worldbank_urbanization_velocity")
        print("Created Gold Table: Urbanization Velocity")

        density_df.write \
            .format("delta") \
            .mode("overwrite") \
            .saveAsTable("default.worldbank_density_spike")
        print("Created Gold Table: Density Spike")

        vulnerable_df.write \
            .format("delta") \
            .mode("overwrite") \
            .saveAsTable("default.worldbank_vulnerable_urbanization")
        print("Created Gold Table: Vulnerable Urbanization")

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
#   1. Urbanization Velocity — 5yr rolling avg growth rate of urban pop %
#   2. Density Spike — absolute density change from 2015 to 2025
#   3. Vulnerable Urbanization — urban growth vs electricity access growth
#   4. Write all three Gold tables as Managed Delta
#   5. Optimize (Z-Order) the velocity table

aggregator = WorldBankAggregator(
    silver_table="default.worldbank_indicators_silver",
)

velocity = aggregator.compute_urbanization_velocity()
density_spike = aggregator.compute_density_spike()
vulnerable = aggregator.compute_vulnerable_urbanization()

aggregator.write_gold_tables(velocity, density_spike, vulnerable)
aggregator.optimize_gold("default.worldbank_urbanization_velocity", "country_iso3")
print("Gold layer aggregation complete.")

display(velocity.limit(20))