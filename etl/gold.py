from pyspark.sql import DataFrame
from pyspark.sql.functions import current_date, year, month, dayofmonth, weekofyear, lit, col, monotonically_increasing_id

def load(df: DataFrame, db_url, db_user, db_password, db_driver):
    """
    Loads data into MySQL Datamart (Gold Layer).
    - Populates Dimensions (Time, Brand, Category, Country, Product)
    - Populates Fact Table
    """
    
    # JDBC Properties
    properties = {
        "user": db_user,
        "password": db_password,
        "driver": db_driver
    }
    
    # 1. Dimension: Brand
    print("Loading dim_brand...")
    brands = df.select("brand_name").distinct().filter(col("brand_name").isNotNull())
    # Coalesce to 1 partition to avoid deadlocks from concurrent writes
    brands.coalesce(1).write.jdbc(url=db_url, table="dim_brand", mode="append", properties=properties) 
    
    # 2. Dimension: Category
    print("Loading dim_category...")
    categories = df.select("category_code").distinct().filter(col("category_code").isNotNull())
    categories.coalesce(1).write.jdbc(url=db_url, table="dim_category", mode="append", properties=properties)
    
    # 3. Dimension: Country
    print("Loading dim_country...")
    countries = df.select(col("country_name").alias("country_name_fr")).distinct().filter(col("country_name_fr").isNotNull())
    countries.coalesce(1).write.jdbc(url=db_url, table="dim_country", mode="append", properties=properties)
    
    # 4. Dimension: Time
    # Check max date or create a date range. For snapshot, current date is relevant?
    # Or use last_modified_t to generate time dimension entries.
    
    # 5. Dimension: Product
    # Needs to lookup foreign keys from dims.
    # This implies we need to read back the dims to get SKs.
    
    # Reading Dims back
    spark = df.sparkSession
    dim_brand = spark.read.jdbc(url=db_url, table="dim_brand", properties=properties)
    dim_category = spark.read.jdbc(url=db_url, table="dim_category", properties=properties)
    # dim_country = ...
    
    # Join to get SKs
    product_df = df.join(dim_brand, df.brand_name == dim_brand.brand_name, "left") \
                   .join(dim_category, df.category_code == dim_category.category_code, "left") \
                   .select(
                       col("code"),
                       col("product_name"),
                       col("brand_sk"),
                       col("category_sk").alias("primary_category_sk")
                   )
                   
    print("Loading dim_product...")
    product_df.coalesce(1).write.jdbc(url=db_url, table="dim_product", mode="append", properties=properties)
    
    # 6. Fact
    # Re-join product to get product_sk
    dim_product = spark.read.jdbc(url=db_url, table="dim_product", properties=properties)
    
    fact_df = df.join(dim_product, "code") \
                .select(
                    col("product_sk"),
                    lit(1).alias("time_sk"), # Placeholder
                    col("energy_kcal_100g"),
                    col("fat_100g"),
                    col("saturated_fat_100g"),
                    col("sugars_100g"),
                    col("salt_100g"),
                    col("proteins_100g"),
                    col("fiber_100g"),
                    col("sodium_100g")
                )
    
    print("Loading fact_nutrition_snapshot...")
    fact_df.coalesce(1).write.jdbc(url=db_url, table="fact_nutrition_snapshot", mode="append", properties=properties)
