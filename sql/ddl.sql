-- DDL for OpenFoodFacts Datamart

-- Dimensions

CREATE TABLE IF NOT EXISTS dim_time (
    time_sk INT AUTO_INCREMENT PRIMARY KEY,
    date DATE UNIQUE NOT NULL,
    year INT,
    month INT,
    day INT,
    week INT,
    iso_week INT
);

CREATE TABLE IF NOT EXISTS dim_brand (
    brand_sk INT AUTO_INCREMENT PRIMARY KEY,
    brand_name VARCHAR(500) NOT NULL,
    INDEX idx_brand_name (brand_name(255))
);

CREATE TABLE IF NOT EXISTS dim_category (
    category_sk INT AUTO_INCREMENT PRIMARY KEY,
    category_code TEXT NOT NULL,
    category_name_fr TEXT,
    level INT,
    parent_category_sk INT,
    FOREIGN KEY (parent_category_sk) REFERENCES dim_category(category_sk)
);

CREATE TABLE IF NOT EXISTS dim_country (
    country_sk INT AUTO_INCREMENT PRIMARY KEY,
    country_code VARCHAR(10),
    country_name_fr TEXT
);

CREATE TABLE IF NOT EXISTS dim_product (
    product_sk INT AUTO_INCREMENT PRIMARY KEY,
    code VARCHAR(50) NOT NULL, -- EAN-13 or other barcode
    product_name VARCHAR(2048),
    brand_sk INT,
    primary_category_sk INT,
    effective_from TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    effective_to TIMESTAMP NULL,
    is_current BOOLEAN DEFAULT TRUE,
    FOREIGN KEY (brand_sk) REFERENCES dim_brand(brand_sk),
    FOREIGN KEY (primary_category_sk) REFERENCES dim_category(category_sk),
    INDEX idx_product_code (code)
);

CREATE TABLE IF NOT EXISTS dim_nutri (
    nutri_sk INT AUTO_INCREMENT PRIMARY KEY,
    nutriscore_grade CHAR(1),
    nova_group INT,
    ecoscore_grade CHAR(1),
    UNIQUE KEY unique_nutri (nutriscore_grade, nova_group, ecoscore_grade)
);

-- Facts

CREATE TABLE IF NOT EXISTS fact_nutrition_snapshot (
    fact_id BIGINT AUTO_INCREMENT PRIMARY KEY,
    product_sk INT NOT NULL,
    time_sk INT NOT NULL,
    
    -- Measures (per 100g)
    energy_kcal_100g FLOAT,
    fat_100g FLOAT,
    saturated_fat_100g FLOAT,
    sugars_100g FLOAT,
    salt_100g FLOAT,
    proteins_100g FLOAT,
    fiber_100g FLOAT,
    sodium_100g FLOAT,
    
    -- Attributes
    nutri_sk INT,
    completeness_score FLOAT,
    
    FOREIGN KEY (product_sk) REFERENCES dim_product(product_sk),
    FOREIGN KEY (time_sk) REFERENCES dim_time(time_sk),
    FOREIGN KEY (nutri_sk) REFERENCES dim_nutri(nutri_sk)
);

CREATE TABLE IF NOT EXISTS bridge_product_category (
    product_sk INT NOT NULL,
    category_sk INT NOT NULL,
    PRIMARY KEY (product_sk, category_sk),
    FOREIGN KEY (product_sk) REFERENCES dim_product(product_sk),
    FOREIGN KEY (category_sk) REFERENCES dim_category(category_sk)
);
