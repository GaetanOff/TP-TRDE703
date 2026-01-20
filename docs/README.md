# OpenFoodFacts Datamart (TP TRDE703)

This project implements an ETL pipeline using Apache Spark (PySpark) to process OpenFoodFacts data and load it into a MySQL Datamart for analysis.

## Project Structure

- `etl/`: PySpark ETL scripts (Bronze, Silver, Gold).
- `sql/`: SQL scripts for Database Schema (DDL) and Analysis.
- `docs/`: Architecture and design documentation.
- `tests/`: Unit tests.
- `conf/`: Configuration files.

## Prerequisites

- Python 3.8+
- Apache Spark 3.x
- Java 8/11/17 (for Spark)
- MySQL 8.0

## Setup

1. **Install Dependencies**:
   ```bash
   pip install -r requirements.txt
   ```

2. **Database Setup**:
   - **Option A: Docker (Recommended)**
     Run the following command to start MySQL and automatically initialize the schema:
     ```bash
     docker-compose up -d
     ```
   
   - **Option B: Manual Local MySQL**
     - Ensure MySQL is running.
     - Run the DDL script to create tables:
       ```bash
       mysql -u root -p < sql/ddl.sql
       ```

3. **Configuration**:
   - Update `etl/config.py` with your database credentials.

## Running the ETL

Execute the main ETL script as a module:
```bash
python3 -m etl.main
```

## Running Tests

Run the unit tests using pytest:
```bash
python3 -m pytest tests/
```

## Analysis

Use the queries in `sql/analysis.sql` to generate insights from the Datamart.

## Troubleshooting

### Java Version
If you encounter `UnsupportedClassVersionError` or `Py4JJavaError`, ensure you are using Java 11 or 17.
```bash
export JAVA_HOME=/path/to/java11
```

### Python Version Mismatch
If you see `[PYTHON_VERSION_MISMATCH]`, set the following environment variables to point to your virtualenv python:
```bash
export PYSPARK_PYTHON=/path/to/venv/bin/python
export PYSPARK_DRIVER_PYTHON=/path/to/venv/bin/python
```
