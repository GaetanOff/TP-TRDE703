# Architecture Note

## Overview
This project follows a Medallion Architecture (Bronze, Silver, Gold) to process OpenFoodFacts data.

## pipeline Stages

### 1. Ingestion (Bronze)
- **Source**: CSV/JSONL Dump from OpenFoodFacts.
- **Process**: Raw ingestion with minimal filtering.
- **Output**: Spark DataFrame (Raw).

### 2. Transformation (Silver)
- **Process**: 
  - Cleaning (Null handling, trimming).
  - Normalization (Units, Uppercase).
  - Deduplication: Keeping the most recent entry based on `last_modified_t`.
- **Output**: clean Spark DataFrame.

### 3. Loading (Gold)
- **Target**: MySQL Datamart (Star Schema).
- **Dimensions**:
  - `dim_product` (SCD Type 1/2 logic via timestamps).
  - `dim_brand`, `dim_category`, `dim_country` (Normalized dictionaries).
  - `dim_time` (Standard date dimension).
- **Facts**:
  - `fact_nutrition_snapshot`: Contains measures like sugar, salt, energy per 100g.

## Technical Choices
- **ETL Engine**: Apache Spark (PySpark) for distributed processing capabilities suitable for "Big Data" (12GB+ source file).
- **Storage**: MySQL for the serving layer (Datamart) to support SQL analytical queries.
- **Upsert Strategy**: 
  - For Dimensions: Insert Ignore / Append.
  - For Facts: Append snapshot data.

## Quality Metrics
- Uniqueness ensured by Deduplication step in Silver.
- Completeness tracked via `completeness_score` in Fact table.
- Anomalies can be flagged in `sql/analysis.sql` queries.
