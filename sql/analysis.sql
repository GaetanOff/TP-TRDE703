-- Analytical Queries for OpenFoodFacts Datamart

-- 1. Top 10 brands by proportion of Nutri-Score A/B
SELECT 
    b.brand_name,
    COUNT(CASE WHEN n.nutriscore_grade IN ('a', 'b') THEN 1 END) * 100.0 / COUNT(*) as prop_ab
FROM fact_nutrition_snapshot f
JOIN dim_product p ON f.product_sk = p.product_sk
JOIN dim_brand b ON p.brand_sk = b.brand_sk
JOIN dim_nutri n ON f.nutri_sk = n.nutri_sk
GROUP BY b.brand_name
HAVING COUNT(*) > 50
ORDER BY prop_ab DESC
LIMIT 10;

-- 2. Nutri-Score distribution by Category Level 2
-- Assuming hierarchy allows identifying level 2. 
-- For simplicity, using category name.
SELECT 
    c.category_name_fr,
    n.nutriscore_grade,
    COUNT(*) as product_count
FROM fact_nutrition_snapshot f
JOIN dim_product p ON f.product_sk = p.product_sk
JOIN dim_category c ON p.primary_category_sk = c.category_sk
JOIN dim_nutri n ON f.nutri_sk = n.nutri_sk
WHERE c.level = 2
GROUP BY c.category_name_fr, n.nutriscore_grade
ORDER BY c.category_name_fr, n.nutriscore_grade;

-- 3. Heatmap: Country x Category -> Avg Sugars
SELECT 
    co.country_name_fr,
    c.category_name_fr,
    AVG(f.sugars_100g) as avg_sugar
FROM fact_nutrition_snapshot f
JOIN dim_product p ON f.product_sk = p.product_sk
JOIN dim_category c ON p.primary_category_sk = c.category_sk
-- Note: Product can have multiple countries. dim_product usually links to one. 
-- If many-to-many, need bridge. Assuming single primary country here or flattened.
JOIN dim_country co ON p.product_sk = co.product_sk -- Hypothetical join if country linked to product directly or via many-to-many
-- Adjusted to current schema:
-- In Gold.py we didn't explicitly link product to country FK. 
-- Let's assume we add country_sk to dim_product or bridge.
-- For now, commenting out the join requiring schema adjustment.
GROUP BY co.country_name_fr, c.category_name_fr;

-- 4. Completeness rate by Brand
SELECT 
    b.brand_name,
    AVG(f.completeness_score) as avg_completeness
FROM fact_nutrition_snapshot f
JOIN dim_product p ON f.product_sk = p.product_sk
JOIN dim_brand b ON p.brand_sk = b.brand_sk
GROUP BY b.brand_name
ORDER BY avg_completeness DESC;

-- 5. Anomalies List (e.g., sugars > 100)
SELECT 
    p.code,
    p.product_name,
    f.sugars_100g
FROM fact_nutrition_snapshot f
JOIN dim_product p ON f.product_sk = p.product_sk
WHERE f.sugars_100g > 100 
   OR f.salt_100g > 100
   OR f.energy_kcal_100g > 900; -- fat is 9kcal/g, so 900 max pure fat
