# Cahier de Qualité des Données

## 1. Règles de Qualité Appliquées

### 1.1 Validation des Valeurs Numériques

| Colonne | Min | Max | Justification |
|---------|-----|-----|---------------|
| `energy_kcal_100g` | 0 | 900 | Max théorique = 100g de graisse pure (9 kcal/g) |
| `fat_100g` | 0 | 100 | Maximum 100g pour 100g de produit |
| `saturated_fat_100g` | 0 | 100 | Maximum 100g pour 100g de produit |
| `sugars_100g` | 0 | 100 | Maximum 100g pour 100g de produit |
| `salt_100g` | 0 | 100 | Maximum 100g pour 100g de produit |
| `proteins_100g` | 0 | 100 | Maximum 100g pour 100g de produit |
| `fiber_100g` | 0 | 100 | Maximum 100g pour 100g de produit |
| `sodium_100g` | 0 | 40 | Maximum ~40g (sel très concentré) |

**Action** : Valeurs hors bornes → `NULL`

### 1.2 Normalisation des Chaînes

| Champ | Transformation |
|-------|---------------|
| `brand_name` | Lowercase + Trim + Unicode NFC + Truncate(500) |
| `country_name` | Premier pays de la liste + Suppression préfixe langue + Mapping ISO |
| `category_code` | Première catégorie + Suppression préfixe + Title Case |
| `nutriscore_grade` | Validation (a, b, c, d, e) sinon `NULL` |

### 1.3 Règles de Déduplication

- **Clé de déduplication** : `code` (code-barres produit)
- **Critère de sélection** : Enregistrement le plus récent (`last_modified_t DESC`)
- **Méthode** : Window function avec `row_number()`

---

## 2. Couverture des Données (Coverage)

### 2.1 Taux de Remplissage par Colonne

```sql
SELECT 
    COUNT(*) as total_products,
    ROUND(COUNT(energy_kcal_100g) * 100.0 / COUNT(*), 2) as pct_energy,
    ROUND(COUNT(sugars_100g) * 100.0 / COUNT(*), 2) as pct_sugars,
    ROUND(COUNT(proteins_100g) * 100.0 / COUNT(*), 2) as pct_proteins,
    ROUND(COUNT(fat_100g) * 100.0 / COUNT(*), 2) as pct_fat
FROM fact_nutrition_snapshot;
```

### 2.2 Répartition des Dimensions

| Dimension | Nombre d'entrées | Description |
|-----------|-----------------|-------------|
| `dim_brand` | ~370 000 | Marques normalisées |
| `dim_category` | ~186 000 | Catégories (première de la hiérarchie) |
| `dim_country` | ~231 | Pays ISO reconnus |
| `dim_product` | ~4.3 M | Produits uniques (dédupliqués) |
| `fact_nutrition_snapshot` | ~4.3 M | Faits nutritionnels |

---

## 3. Anomalies Détectées et Traitées

### 3.1 Valeurs Numériques Aberrantes

| Type d'anomalie | Exemple avant nettoyage | Action |
|----------------|------------------------|--------|
| Valeurs négatives | `energy_kcal_100g = -8801148750802.9` | → `NULL` |
| Valeurs extrêmes | `fat_100g = 46465700000000000000000000` | → `NULL` |
| Infinity | `sugars_100g = Infinity` | → `NULL` |
| NaN | `proteins_100g = NaN` | → `NULL` |

### 3.2 Pays Non Standardisés

| Problème | Exemples | Action |
|----------|----------|--------|
| Noms multilingues | ประเทศสเปน, Германия, 法国, フランス | Mapping → Nom anglais ISO |
| Listes de pays | "France,Belgium,Germany" | → Premier pays uniquement |
| Préfixes de langue | "en:france", "fr:allemagne" | Suppression du préfixe |
| Données garbage | "Hello", "0546578655", "Bbbb" | → `NULL` |

### 3.3 Marques Dupliquées

| Problème | Exemple | Action |
|----------|---------|--------|
| Casse variée | "Nestlé" vs "NESTLÉ" vs "nestlé" | Normalisation lowercase |
| Accents Unicode | "Café" (NFC) vs "Café" (NFD) | Normalisation NFC |
| Espaces | " Carrefour " vs "Carrefour" | Trim |

---

## 4. Comparaison Before/After

### 4.1 Nombre de Pays

| Métrique | Avant | Après |
|----------|-------|-------|
| Entrées `dim_country` | **21 579** | **231** |
| Réduction | - | **99%** |

**Cause** : Les données brutes contenaient des noms de pays dans toutes les langues + données garbage.

### 4.2 Valeurs Nutritionnelles

| Métrique | Avant nettoyage | Après nettoyage |
|----------|-----------------|-----------------|
| `AVG(energy_kcal_100g)` | -1 265 465 791 985 | ~240 |
| `AVG(sugars_100g)` | 4.0 × 10²⁵ | ~10 |
| `AVG(proteins_100g)` | 18 854 | ~8 |

### 4.3 Requête de Validation

```sql
-- Vérification des bornes respectées
SELECT 
    MIN(energy_kcal_100g) as min_energy,
    MAX(energy_kcal_100g) as max_energy,
    MIN(sugars_100g) as min_sugars,
    MAX(sugars_100g) as max_sugars,
    MIN(fat_100g) as min_fat,
    MAX(fat_100g) as max_fat
FROM fact_nutrition_snapshot;
-- Résultat attendu : toutes les valeurs entre 0 et leur borne max respective
```

---

## 5. Métriques de Qualité

### 5.1 Indicateurs Clés

| KPI | Formule | Cible |
|-----|---------|-------|
| Complétude énergie | `COUNT(energy) / COUNT(*)` | > 70% |
| Validité pays | `COUNT(valid_country) / COUNT(*)` | 100% |
| Unicité produits | `COUNT(DISTINCT code) = COUNT(*)` | 100% |
| Intégrité FK | Toutes FK résolues | 100% |

### 5.2 Requête de Monitoring

```sql
-- Dashboard qualité
SELECT 
    (SELECT COUNT(*) FROM dim_brand) as brands,
    (SELECT COUNT(*) FROM dim_category) as categories,
    (SELECT COUNT(*) FROM dim_country) as countries,
    (SELECT COUNT(*) FROM dim_product) as products,
    (SELECT COUNT(*) FROM fact_nutrition_snapshot) as facts,
    (SELECT COUNT(*) FROM fact_nutrition_snapshot WHERE energy_kcal_100g IS NOT NULL) as with_energy,
    (SELECT COUNT(*) FROM fact_nutrition_snapshot WHERE sugars_100g IS NOT NULL) as with_sugars;
```

---

## 6. Règles Métier Spécifiques

### 6.1 Nutri-Score

- **Valeurs acceptées** : a, b, c, d, e (lowercase)
- **Autres valeurs** : → `NULL`
- **Logique** : Conservation uniquement des grades officiels

### 6.2 Codes Pays ISO

- **Standard** : ISO 3166-1 alpha-2
- **Source** : Bibliothèque `pycountry`
- **Exceptions** : 
  - "World" → `WW` (code personnalisé)
  - "Russia" → `RU` (alias pour "Russian Federation")
  - "Turkey" → `TR` (alias pour "Türkiye")

---

## 7. Processus de Nettoyage (Pipeline)

```
Bronze (Raw)
    │
    ▼
┌─────────────────────────────────────────┐
│ Silver Layer - Transformations          │
├─────────────────────────────────────────┤
│ 1. Sélection des colonnes               │
│ 2. Typage (cast float, string)          │
│ 3. Normalisation marques (NFC, lower)   │
│ 4. Mapping pays (pycountry)             │
│ 5. Extraction première catégorie        │
│ 6. Validation Nutri-Score               │
│ 7. Bornes numériques (NULL si hors)     │
│ 8. Déduplication par code               │
└─────────────────────────────────────────┘
    │
    ▼
Gold (Datamart MySQL)
```

---

## 8. Conclusion

Le pipeline ETL applique **15+ règles de qualité** couvrant :
- ✅ Validation des bornes physiques
- ✅ Normalisation Unicode
- ✅ Standardisation des pays (ISO)
- ✅ Déduplication
- ✅ Gestion des valeurs manquantes
- ✅ Filtrage des données garbage

**Résultat** : Données exploitables pour analyses nutritionnelles fiables.
