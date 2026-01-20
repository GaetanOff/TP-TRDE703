from pyspark.sql import SparkSession
from etl import config
from etl import bronze, silver, gold
import sys

def main():
    spark = SparkSession.builder \
        .appName(config.APP_NAME) \
        .master(config.MASTER) \
        .config("spark.jars.packages", "mysql:mysql-connector-java:8.0.33") \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("WARN")
    
    try:
        print("Starting Bronze Layer...")
        raw_df = bronze.extract(spark, config.INPUT_PATH)
        
        print("Starting Silver Layer...")
        clean_df = silver.transform(raw_df)
        
        print("Starting Gold Layer...")
        gold.load(clean_df, config.DB_URL, config.DB_USER, config.DB_PASSWORD, config.DB_DRIVER)
        
        print("ETL Job Finished Successfully")
        
    except Exception as e:
        print(f"ETL Job Failed: {e}")
        sys.exit(1)
    finally:
        spark.stop()

if __name__ == "__main__":
    main()
