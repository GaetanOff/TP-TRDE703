-- Analytical Queries for OpenFoodFacts Datamart
-- Note: These queries work with the current schema where nutrition values 
-- are stored directly in fact_nutrition_snapshot

-- 1. Top 10 brands by number of products
SELECT 
    b.brand_name,
    COUNT(*) as product_count
FROM dim_product p
JOIN dim_brand b ON p.brand_sk = b.brand_sk
GROUP BY b.brand_name
HAVING COUNT(*) > 50
ORDER BY product_count DESC
LIMIT 10;

-- 2. Average nutritional values by brand (Top brands)
SELECT 
    b.brand_name,
    COUNT(*) as product_count,
    ROUND(AVG(f.energy_kcal_100g), 2) as avg_energy,
    ROUND(AVG(f.fat_100g), 2) as avg_fat,
    ROUND(AVG(f.sugars_100g), 2) as avg_sugars,
    ROUND(AVG(f.proteins_100g), 2) as avg_proteins
FROM fact_nutrition_snapshot f
JOIN dim_product p ON f.product_sk = p.product_sk
JOIN dim_brand b ON p.brand_sk = b.brand_sk
WHERE f.energy_kcal_100g IS NOT NULL
GROUP BY b.brand_name
HAVING COUNT(*) > 100
ORDER BY product_count DESC
LIMIT 20;

-- 3. Product distribution by category
SELECT 
    c.category_code,
    COUNT(*) as product_count
FROM dim_product p
JOIN dim_category c ON p.primary_category_sk = c.category_sk
GROUP BY c.category_code
ORDER BY product_count DESC
LIMIT 20;

-- 4. Average sugars by category (Top categories by product count)
SELECT 
    c.category_code,
    COUNT(*) as product_count,
    ROUND(AVG(f.sugars_100g), 2) as avg_sugars_100g,
    ROUND(AVG(f.salt_100g), 2) as avg_salt_100g
FROM fact_nutrition_snapshot f
JOIN dim_product p ON f.product_sk = p.product_sk
JOIN dim_category c ON p.primary_category_sk = c.category_sk
WHERE f.sugars_100g IS NOT NULL
GROUP BY c.category_code
HAVING COUNT(*) > 50
ORDER BY avg_sugars_100g DESC
LIMIT 20;

-- 5. Anomalies List (e.g., sugars > 100, excessive energy values)
SELECT 
    p.code,
    p.product_name,
    f.energy_kcal_100g,
    f.sugars_100g,
    f.salt_100g
FROM fact_nutrition_snapshot f
JOIN dim_product p ON f.product_sk = p.product_sk
WHERE f.sugars_100g > 100 
   OR f.salt_100g > 100
   OR f.energy_kcal_100g > 900
LIMIT 50;

-- 6. Country distribution (products by country)
SELECT 
    country_name_fr,
    COUNT(*) as appearance_count
FROM dim_country
WHERE country_name_fr IS NOT NULL
GROUP BY country_name_fr
ORDER BY appearance_count DESC
LIMIT 20;

-- 7. Products with highest fat content
SELECT 
    p.code,
    p.product_name,
    b.brand_name,
    f.fat_100g,
    f.saturated_fat_100g
FROM fact_nutrition_snapshot f
JOIN dim_product p ON f.product_sk = p.product_sk
LEFT JOIN dim_brand b ON p.brand_sk = b.brand_sk
WHERE f.fat_100g IS NOT NULL
ORDER BY f.fat_100g DESC
LIMIT 20;

-- 8. Products with highest protein content (healthy options)
SELECT 
    p.code,
    p.product_name,
    b.brand_name,
    f.proteins_100g,
    f.energy_kcal_100g
FROM fact_nutrition_snapshot f
JOIN dim_product p ON f.product_sk = p.product_sk
LEFT JOIN dim_brand b ON p.brand_sk = b.brand_sk
WHERE f.proteins_100g IS NOT NULL
ORDER BY f.proteins_100g DESC
LIMIT 20;

-- 9. Summary statistics
SELECT 
    COUNT(*) as total_facts,
    COUNT(energy_kcal_100g) as with_energy,
    COUNT(sugars_100g) as with_sugars,
    COUNT(proteins_100g) as with_proteins,
    ROUND(AVG(energy_kcal_100g), 2) as avg_energy,
    ROUND(AVG(sugars_100g), 2) as avg_sugars,
    ROUND(AVG(proteins_100g), 2) as avg_proteins
FROM fact_nutrition_snapshot;

-- 10. Brand overview with all dimensions
SELECT 
    (SELECT COUNT(*) FROM dim_brand) as total_brands,
    (SELECT COUNT(*) FROM dim_category) as total_categories,
    (SELECT COUNT(*) FROM dim_country) as total_countries,
    (SELECT COUNT(*) FROM dim_product) as total_products,
    (SELECT COUNT(*) FROM fact_nutrition_snapshot) as total_facts;
