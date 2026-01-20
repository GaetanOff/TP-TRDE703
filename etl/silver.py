from pyspark.sql import DataFrame
from pyspark.sql.functions import col, when, coalesce, lit, current_timestamp, split, regexp_replace, lower, trim


def transform(df: DataFrame) -> DataFrame:
    """
    Transforms the Bronze dataframe to Silver.
    - Selects relevant columns
    - Casts types
    - Cleans data
    """
    
    # 1. Select relevant columns and rename if necessary
    # Assuming columns exist in the CSV. In a real scenario, we'd check for existence.
    cols_to_select = [
        col("code"),
        col("product_name").alias("product_name"),
        col("brands").alias("brand_name"),
        col("categories_tags").alias("category_code"), # Using tags as code often
        col("countries_fr").alias("country_name"),
        col("nutriscore_grade"),
        col("nova_group"),
        col("ecoscore_grade_fr").alias("ecoscore_grade"),
        col("energy-kcal_100g").cast("float").alias("energy_kcal_100g"),
        col("fat_100g").cast("float"),
        col("saturated-fat_100g").cast("float").alias("saturated_fat_100g"),
        col("sugars_100g").cast("float"),
        col("salt_100g").cast("float"),
        col("proteins_100g").cast("float"),
        col("fiber_100g").cast("float"),
        col("sodium_100g").cast("float"),
        col("last_modified_t")
    ]
    
    # Note: Column names in the CSV might vary (hyphens vs underscores).
    # We might need to adjust based on actual CSV header.
    # For now, we proceed with standard assumptions from OFF data.
    
    # Safe selection (ignoring missing columns could be an option, but for now we try to select)
    # To make it robust, we should check df.columns
    existing_cols = df.columns
    selected_cols = []
    
    # Mapping for potential column names
    mapping = {
        "code": ["code"],
        "product_name": ["product_name", "product_name_fr", "product_name_en"],
        "brand_name": ["brands", "brands_tags"],
        "category_code": ["categories_tags", "main_category_fr"],
        "country_name": ["countries_fr", "countries", "countries_en"],
        "nutriscore_grade": ["nutriscore_grade"],
        "nova_group": ["nova_group"],
        "ecoscore_grade": ["ecoscore_grade_fr", "ecoscore_grade"],
        "energy_kcal_100g": ["energy-kcal_100g", "energy_100g"],
        "fat_100g": ["fat_100g"],
        "saturated_fat_100g": ["saturated-fat_100g"],
        "sugars_100g": ["sugars_100g"],
        "salt_100g": ["salt_100g"],
        "proteins_100g": ["proteins_100g"],
        "fiber_100g": ["fiber_100g"],
        "sodium_100g": ["sodium_100g"],
        "last_modified_t": ["last_modified_t"]
    }
    
    # Helper to find first existing column from a list
    def get_col(candidates, alias):
        for c in candidates:
            if c in existing_cols:
                return col(c).alias(alias)
        return lit(None).alias(alias)

    transformed_df = df.select(
        col("code"),
        get_col(mapping["product_name"], "product_name"),
        get_col(mapping["brand_name"], "brand_name"),
        get_col(mapping["category_code"], "category_code"),
        get_col(mapping["country_name"], "country_name"),
        get_col(mapping["nutriscore_grade"], "nutriscore_grade"),
        get_col(mapping["nova_group"], "nova_group"),
        get_col(mapping["ecoscore_grade"], "ecoscore_grade"),
        get_col(mapping["energy_kcal_100g"], "energy_kcal_100g").cast("float"),
        get_col(mapping["fat_100g"], "fat_100g").cast("float"),
        get_col(mapping["saturated_fat_100g"], "saturated_fat_100g").cast("float"),
        get_col(mapping["sugars_100g"], "sugars_100g").cast("float"),
        get_col(mapping["salt_100g"], "salt_100g").cast("float"),
        get_col(mapping["proteins_100g"], "proteins_100g").cast("float"),
        get_col(mapping["fiber_100g"], "fiber_100g").cast("float"),
        get_col(mapping["sodium_100g"], "sodium_100g").cast("float"),
        get_col(mapping["last_modified_t"], "last_modified_t")
    )
    
    # Cleanups
    # 1. Fill brands nulls, normalize unicode, lowercase, trim, and truncate to 500 chars
    from pyspark.sql.functions import substring, udf, split, regexp_replace
    from pyspark.sql.types import StringType
    import unicodedata
    
    @udf(returnType=StringType())
    def normalize_unicode(s):
        if s is None:
            return "unknown"
        # NFC normalization to handle composed vs decomposed characters
        return unicodedata.normalize('NFC', s.lower().strip())[:500]
    
    # Use pycountry for comprehensive country lookup
    import pycountry
    
    # Build mappings: name -> standardized name, and name -> ISO code
    COUNTRY_MAP = {}
    COUNTRY_CODE_MAP = {}  # Maps standardized country name to ISO alpha_2 code
    
    for country in pycountry.countries:
        name_en = country.name
        code = country.alpha_2
        # Add to code map
        COUNTRY_CODE_MAP[name_en] = code
        # Add official name
        COUNTRY_MAP[name_en.lower()] = name_en
        # Add alpha_2 code
        COUNTRY_MAP[country.alpha_2.lower()] = name_en
        # Add alpha_3 code
        COUNTRY_MAP[country.alpha_3.lower()] = name_en
        # Add common name if exists
        if hasattr(country, 'common_name'):
            COUNTRY_MAP[country.common_name.lower()] = name_en
        # Add official name if exists
        if hasattr(country, 'official_name'):
            COUNTRY_MAP[country.official_name.lower()] = name_en
    
    # Add extra common variations not covered by pycountry
    EXTRA_MAP = {
        'royaume-uni': 'United Kingdom', 'angleterre': 'United Kingdom', 'england': 'United Kingdom',
        'etats-unis': 'United States', 'amerika': 'United States', 'usa': 'United States',
        'allemagne': 'Germany', 'alemania': 'Germany', 'niemcy': 'Germany', 'duitsland': 'Germany',
        'espagne': 'Spain', 'spanien': 'Spain', 'spagna': 'Spain',
        'italie': 'Italy', 'italien': 'Italy',
        'belgique': 'Belgium', 'belgie': 'Belgium', 'belgien': 'Belgium',
        'pays-bas': 'Netherlands', 'holland': 'Netherlands', 'nederland': 'Netherlands',
        'suisse': 'Switzerland', 'schweiz': 'Switzerland', 'svizzera': 'Switzerland',
        'autriche': 'Austria', 'osterreich': 'Austria', 'oostenrijk': 'Austria',
        'pologne': 'Poland', 'polska': 'Poland', 'polen': 'Poland',
        'suede': 'Sweden', 'schweden': 'Sweden', 'sverige': 'Sweden',
        'norvege': 'Norway', 'norwegen': 'Norway', 'norge': 'Norway',
        'danemark': 'Denmark', 'danmark': 'Denmark',
        'finlande': 'Finland', 'finnland': 'Finland', 'suomi': 'Finland',
        'irlande': 'Ireland', 'irland': 'Ireland',
        'grece': 'Greece', 'griechenland': 'Greece', 'grecia': 'Greece',
        'roumanie': 'Romania', 'rumanien': 'Romania',
        'hongrie': 'Hungary', 'ungarn': 'Hungary',
        'republique tcheque': 'Czechia', 'tschechien': 'Czechia',
        'maroc': 'Morocco', 'marokko': 'Morocco',
        'tunisie': 'Tunisia', 'tunesien': 'Tunisia',
        'algerie': 'Algeria', 'algerien': 'Algeria',
        'afrique du sud': 'South Africa', 'sudafrika': 'South Africa',
        'turquie': 'Turkey', 'turkei': 'Turkey',
        'bresil': 'Brazil', 'brasilien': 'Brazil',
        'mexique': 'Mexico', 'mexiko': 'Mexico',
        'chine': 'China', 'japon': 'Japan', 'inde': 'India', 'indien': 'India',
        'russie': 'Russia', 'russland': 'Russia',
        'australie': 'Australia', 'australien': 'Australia',
        'world': 'World', 'monde': 'World', 'worldwide': 'World', 'welt': 'World',
    }
    COUNTRY_MAP.update(EXTRA_MAP)
    
    @udf(returnType=StringType())
    def clean_country(s):
        if s is None:
            return None
        # Take first country from comma-separated list
        first = s.split(',')[0].strip()
        # Remove language prefix like "en:", "fr:", etc.
        if ':' in first:
            first = first.split(':')[-1]
        # Remove accents and normalize
        first = ''.join(c for c in unicodedata.normalize('NFKD', first) if not unicodedata.combining(c))
        first = first.strip().lower().replace('-', ' ')
        # Map to standardized name, return None if not recognized
        return COUNTRY_MAP.get(first, None)
    
    @udf(returnType=StringType())
    def clean_category(s):
        if s is None:
            return None
        # Take first category from comma-separated list
        first = s.split(',')[0].strip()
        # Remove language prefix
        if ':' in first:
            first = first.split(':')[-1]
        # Remove accents and clean up
        first = ''.join(c for c in unicodedata.normalize('NFKD', first) if not unicodedata.combining(c))
        return first.strip().replace('-', ' ').title() if first else None
    
    @udf(returnType=StringType())
    def get_country_code(country_name):
        """Get ISO alpha-2 code from standardized country name"""
        if country_name is None:
            return None
        return COUNTRY_CODE_MAP.get(country_name, None)
    
    transformed_df = transformed_df.withColumn("brand_name", normalize_unicode(coalesce(col("brand_name"), lit("unknown"))))
    transformed_df = transformed_df.withColumn("country_name", clean_country(col("country_name")))
    transformed_df = transformed_df.withColumn("country_code", get_country_code(col("country_name")))
    transformed_df = transformed_df.withColumn("category_code", clean_category(col("category_code")))
    
    # 2. Nutriscore uppercase
    transformed_df = transformed_df.withColumn("nutriscore_grade", when(col("nutriscore_grade").isin("a", "b", "c", "d", "e"), col("nutriscore_grade")).otherwise(None))
    
    # 3. Data Quality: Replace Infinity, NaN, and out-of-bounds values with NULL
    # Nutritional values per 100g cannot exceed 100g, energy cannot exceed ~900 kcal (pure fat)
    from pyspark.sql.functions import isnan
    
    # Define valid bounds for each column (min, max)
    bounds = {
        "energy_kcal_100g": (0, 900),      # Max ~900 kcal for pure fat
        "fat_100g": (0, 100),              # Max 100g per 100g
        "saturated_fat_100g": (0, 100),    # Max 100g per 100g
        "sugars_100g": (0, 100),           # Max 100g per 100g
        "salt_100g": (0, 100),             # Max 100g per 100g
        "proteins_100g": (0, 100),         # Max 100g per 100g
        "fiber_100g": (0, 100),            # Max 100g per 100g
        "sodium_100g": (0, 40),            # Max ~40g (very salty)
    }
    
    for nc, (min_val, max_val) in bounds.items():
        transformed_df = transformed_df.withColumn(
            nc, 
            when(
                (col(nc).isNull()) | 
                isnan(col(nc)) | 
                (col(nc) < min_val) | 
                (col(nc) > max_val),
                None
            ).otherwise(col(nc))
        )
    
    # 3. Deduplicate by code taking the latest modification
    # Window function to get latest
    from pyspark.sql.window import Window
    from pyspark.sql.functions import row_number
    
    windowSpec = Window.partitionBy("code").orderBy(col("last_modified_t").desc())
    
    deduped_df = transformed_df.withColumn("rn", row_number().over(windowSpec)) \
        .filter(col("rn") == 1) \
        .drop("rn")
        
    return deduped_df
