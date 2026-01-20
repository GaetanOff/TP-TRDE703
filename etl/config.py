# ETL Configuration

# Files
INPUT_PATH = "en.openfoodfacts.org.products.csv"
OUTPUT_PATH = "output/"

# Database
DB_URL = "jdbc:mysql://localhost:3306/off_datamart"
DB_USER = "root"
DB_PASSWORD = "password" # Change as needed
DB_DRIVER = "com.mysql.cj.jdbc.Driver"

# Spark
APP_NAME = "OpenFoodFactsETL"
MASTER = "local[*]"
