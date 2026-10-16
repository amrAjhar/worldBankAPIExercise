# World Bank Demographic & Infrastructure Pipeline

![Databricks](https://img.shields.io/badge/Databricks-Serverless-FF3621?logo=databricks)
![PySpark](https://img.shields.io/badge/PySpark-Data_Engineering-E25A1C?logo=apachespark)
![Delta Lake](https://img.shields.io/badge/Delta_Lake-Medallion-00AAD2)

## Overview
An end-to-end data engineering pipeline built on **Databricks Serverless** that extracts human-activity and spatial-trend indicators from the World Bank API. The pipeline processes data through a Medallion architecture (Bronze → Silver → Gold) to produce business-ready analytical tables identifying urbanization patterns, density shifts, and infrastructure gaps across 261 countries (1960–2025).

*![Insert Screenshot of Databricks DAG here]*

## Architecture & Data Flow

The pipeline is orchestrated as a 3-task Databricks Workflow with a strict dependency chain. 

```text
World Bank API (3 indicators)
        │
        ▼
┌──────────────────┐     ┌──────────────────┐     ┌──────────────────────────┐
│   BRONZE         │────▶│   SILVER         │────▶│   GOLD (3 tables)        │
│   Raw Extract    │     │   Clean + Type   │     │   Analytics + Rankings   │
│   52,668 rows    │     │   51,678 rows    │     │   261 + 214 + 15 rows    │
│   Managed Delta  │     │   Managed Delta  │     │   Managed Delta + Z-Order│
└──────────────────┘     └──────────────────┘     └──────────────────────────┘
