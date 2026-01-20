# ETL Configuration
import os

# Files
INPUT_PATH = os.getenv("INPUT_PATH", "en.openfoodfacts.org.products.csv")
OUTPUT_PATH = os.getenv("OUTPUT_PATH", "output/")

# Database - Use environment variables for Docker, with localhost defaults for local dev
DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = os.getenv("DB_PORT", "3306")
DB_NAME = os.getenv("DB_NAME", "off_datamart")
DB_URL = f"jdbc:mysql://{DB_HOST}:{DB_PORT}/{DB_NAME}"
DB_USER = os.getenv("DB_USER", "root")
DB_PASSWORD = os.getenv("DB_PASSWORD", "password")
DB_DRIVER = "com.mysql.cj.jdbc.Driver"

# Spark
APP_NAME = "OpenFoodFactsETL"
MASTER = "local[*]"
