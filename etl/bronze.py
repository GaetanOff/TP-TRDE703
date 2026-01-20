from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import StructType, StructField, StringType, FloatType, LongType

def extract(spark: SparkSession, file_path: str) -> DataFrame:
    """
    Reads the raw CSV data with a defined schema or infers it.
    For this large file, we will try to infer a subset or use a predefined schema logic if strictness is required.
    For the TP, prompt suggests reading CSV with explicit schema is better but CSV is messy.
    We will read with header=True and mostly StringType to start, then cast in Silver.
    """
    
    # Reading as CSV
    # Using specific options for handling messy CSVs
    df = spark.read \
        .option("header", "true") \
        .option("sep", "\t") \
        .option("inferSchema", "false") \
        .csv(file_path)
    
    # Basic filtering to ensure we have meaningful rows (e.g., must have code)
    df_filtered = df.filter(df.code.isNotNull())
    
    return df_filtered
