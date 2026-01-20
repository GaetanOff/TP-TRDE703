import pytest
from pyspark.sql import SparkSession
from etl import silver

@pytest.fixture(scope="session")
def spark():
    return SparkSession.builder \
        .master("local[1]") \
        .appName("TestETL") \
        .getOrCreate()

def test_silver_transformation(spark):
    # Create dummy bronze data
    data = [
        ("123", "Product A", "BrandX", "Cat1", "France", "a", 4, "A", "100", "10", "1", "5", "1", "2", "3", "0.4", 1000),
        ("123", "Product A New", "BrandX", "Cat1", "France", "b", 4, "B", "100", "10", "1", "5", "1", "2", "3", "0.4", 2000), # Newer
        ("456", "Product B", "BrandY", "Cat2", "USA", "e", 1, "E", "200", "20", "2", "10", "2", "4", "6", "0.8", 1500)
    ]
    columns = ["code", "product_name", "brands", "categories_tags", "countries_fr", "nutriscore_grade", "nova_group", "ecoscore_grade_fr", 
               "energy-kcal_100g", "fat_100g", "saturated-fat_100g", "sugars_100g", "salt_100g", "proteins_100g", "fiber_100g", "sodium_100g", "last_modified_t"]
    
    df = spark.createDataFrame(data, columns)
    
    # Run transformation
    result_df = silver.transform(df)
    
    # Assertions
    results = result_df.collect()
    
    # Should be 2 rows (deduplicated '123_BrandX' keeping the one with ts 2000)
    assert len(results) == 2
    
    # Check deduplication logic (should have 'Product A New')
    prod_a = [row for row in results if row.code == "123"][0]
    assert prod_a.product_name == "Product A New"
    assert prod_a.nutriscore_grade == "b"
    
    # Check types
    assert isinstance(prod_a.energy_kcal_100g, float)

