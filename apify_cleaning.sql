-- SQL cleaning script for Apify real-estate dataset
-- Author: Markéta Vorlová
-- Repository: project-5-apify-cleaning
-- -------------------------------------------------

------------------------------------------------------------------------------------------------------------------
-- STEP 0: Verify that we are working with the correct table
------------------------------------------------------------------------------------------------------------------

-- Goal:
--   Inspect all columns in the table to confirm that it contains the expected data
--   (e.g., information about real-estate listings).

-- Purpose:
--   Ensure that we are using the correct table imported from Keboola
--   and verify that the data was successfully loaded into Snowflake.

-- Set the database and schema
USE DATABASE "KBC_EUW3_2105";
USE SCHEMA "WORKSPACE_38986459";

-- List all column names in the table
SHOW COLUMNS IN "Apify_dataset";

-- Preview the first 5 rows of the table
SELECT *
FROM KBC_EUW3_2105."in.c-apify-apify-01k7vb83wnr044yxwkakahqma8"."dataset-items"
LIMIT 5;

-- Interpretation:
--   The output should display several example listings (e.g., price, location,
--   property type, etc.). If the data structure matches expectations,
--   we can proceed to STEP 1 – Initial exploration of the dataset.
------------------------------------------------------------------------------------------------------------------

------------------------------------------------------------------------------------------------------------------
-- STEP 1: Initial exploration of the dataset
------------------------------------------------------------------------------------------------------------------

-- Goal:
--   Identify which types of properties are included in the dataset
--   (e.g., apartments, houses, land plots, cottages).

-- Purpose:
--   Understand the composition of the dataset and decide whether
--   some property types (e.g., recreational objects) should be excluded
--   from the analytical dataset.

-- Select all distinct values from the "data_type" column
SELECT DISTINCT "data_type"
FROM KBC_EUW3_2105."in.c-apify-apify-01k7vb83wnr044yxwkakahqma8"."dataset-items";

-- Interpretation:
--   • If the output contains values such as 'apartment' and 'house',
--     the dataset includes only residential listings.
--   • If additional types appear (e.g., 'cottage', 'land', 'garden'),
--     we will need to decide whether to exclude them from further analysis.
------------------------------------------------------------------------------------------------------------------

------------------------------------------------------------------------------------------------------------------
-- STEP 2: Count all records in the original table
------------------------------------------------------------------------------------------------------------------

-- Goal:
--   Determine the total number of rows in the Apify dataset,
--   regardless of property type.

-- Purpose:
--   This number will later be compared with the row count after filtering.

SELECT COUNT(*) AS total_rows_all
FROM KBC_EUW3_2105."in.c-apify-apify-01k7vb83wnr044yxwkakahqma8"."dataset-items";

-- Interpretation:
--   This value represents all listings in the dataset
--   (apartments, houses, land plots, commercial properties, etc.).
------------------------------------------------------------------------------------------------------------------

------------------------------------------------------------------------------------------------------------------
-- STEP 3: Create a cleaned table containing only residential properties
------------------------------------------------------------------------------------------------------------------

-- Goal:
--   Select only apartments ('apartment') and houses ('house').

-- Purpose:
--   Remove irrelevant property types such as land plots, commercial spaces,
--   or other non-residential categories.

-- Create a temporary table with residential listings only
CREATE OR REPLACE TEMPORARY TABLE STG_APIFY_CLEAN AS
SELECT *
FROM KBC_EUW3_2105."in.c-apify-apify-01k7vb83wnr044yxwkakahqma8"."dataset-items"
WHERE "data_type" IN ('apartment', 'house');

-- Check how many rows remain after filtering
SELECT COUNT(*) AS total_rows_clean
FROM STG_APIFY_CLEAN;

------------------------------------------------------------------------------------------------------------------
-- NOTE: Status after filtering
--
-- Example values:
--   • original table: 4,269,262 rows
--   • after filtering (apartments + houses): 3,008,503 rows
--
--   This means that approximately 29.5% of records were removed
--   (land plots, commercial properties, and other non-residential listings).
------------------------------------------------------------------------------------------------------------------

------------------------------------------------------------------------------------------------------------------
-- STEP 4: Check for duplicates based on the "id" column
------------------------------------------------------------------------------------------------------------------

-- Goal:
--   Identify whether the dataset contains duplicate listings (same "id").

-- Purpose:
--   Duplicate records can distort property counts and affect average price calculations.

SELECT 
    "id",
    COUNT(*) AS count_duplicate
FROM STG_APIFY_CLEAN
GROUP BY "id"
HAVING COUNT(*) > 1;

-- Interpretation:
--   • If the query returns an empty result → no duplicates exist.
--   • If it returns rows → these IDs are duplicated and must be cleaned.
------------------------------------------------------------------------------------------------------------------


------------------------------------------------------------------------------------------------------------------
-- STEP 5: Remove duplicates (final method)
------------------------------------------------------------------------------------------------------------------

-- Goal:
--   Keep only one record per unique "id".

-- Purpose:
--   A simple DISTINCT is not sufficient because duplicated listings often differ
--   in other fields (e.g., update timestamps). ROW_NUMBER() gives full control.

-- Method:
--   Use ROW_NUMBER() to keep only the first occurrence of each "id".

CREATE OR REPLACE TABLE STG_APIFY_CLEAN_UNIQUE AS
SELECT *
FROM STG_APIFY_CLEAN
QUALIFY ROW_NUMBER() OVER (PARTITION BY "id" ORDER BY "id") = 1;

-- Verify row counts after duplicate removal
SELECT 
    COUNT(*) AS total_rows_after_cleaning,
    COUNT(DISTINCT "id") AS distinct_ids_after_cleaning
FROM STG_APIFY_CLEAN_UNIQUE;

-- Interpretation:
--   • If both numbers match → duplicates were successfully removed.
--   • If they differ → duplicate issues remain and require further investigation.
------------------------------------------------------------------------------------------------------------------

-- NOTE:
--   Duplicate listings have been successfully removed.
--   The total number of rows equals the number of distinct IDs,
--   confirming that the dataset is now consistent and ready for further cleaning.

------------------------------------------------------------------------------------------------------------------
-- STEP 6: Check missing values in key fields
------------------------------------------------------------------------------------------------------------------

-- Goal:
--   Identify how many rows are missing essential information
--   (price, living area, district, GPS coordinates).

-- Purpose:
--   Records missing these attributes may be unusable for calculating price per m²
--   or for geographic analysis (e.g., mapping by region).

-- Note:
--   Values such as '' (empty string) or '.' are treated as missing,
--   the same way as NULL.

WITH stats AS (
  SELECT
    COUNT(*) AS total,
    SUM(CASE WHEN "data_priceTotal"   IS NULL OR "data_priceTotal"   IN ('', '.') THEN 1 ELSE 0 END) AS price_nulls,
    SUM(CASE WHEN "data_livingArea"   IS NULL OR "data_livingArea"   IN ('', '.') THEN 1 ELSE 0 END) AS living_nulls,
    SUM(CASE WHEN "data_district"     IS NULL OR "data_district"     IN ('', '.') THEN 1 ELSE 0 END) AS district_nulls,
    SUM(CASE WHEN "data_gpsCoord_lat" IS NULL OR "data_gpsCoord_lat" IN ('', '.') THEN 1 ELSE 0 END) AS lat_nulls,
    SUM(CASE WHEN "data_gpsCoord_lon" IS NULL OR "data_gpsCoord_lon" IN ('', '.') THEN 1 ELSE 0 END) AS lon_nulls
  FROM STG_APIFY_CLEAN_UNIQUE
)

SELECT * FROM (
  SELECT 'data_priceTotal' AS column_name, price_nulls AS nulls, total,
         ROUND(price_nulls * 100.0 / NULLIF(total, 0), 2) AS null_pct
  FROM stats

  UNION ALL
  SELECT 'data_livingArea', living_nulls, total,
         ROUND(living_nulls * 100.0 / NULLIF(total, 0), 2)
  FROM stats

  UNION ALL
  SELECT 'data_district', district_nulls, total,
         ROUND(district_nulls * 100.0 / NULLIF(total, 0), 2)
  FROM stats

  UNION ALL
  SELECT 'data_gpsCoord_lat', lat_nulls, total,
         ROUND(lat_nulls * 100.0 / NULLIF(total, 0), 2)
  FROM stats

  UNION ALL
  SELECT 'data_gpsCoord_lon', lon_nulls, total,
         ROUND(lon_nulls * 100.0 / NULLIF(total, 0), 2)
  FROM stats
) AS t
ORDER BY null_pct DESC;

-- Interpretation:
--   Columns with the highest percentage of missing values will be addressed first
--   (e.g., filtering, imputation, or separate analysis).
------------------------------------------------------------------------------------------------------------------

------------------------------------------------------------------------------------------------------------------
-- STEP 7: Analysis of missing or suspicious values in key fields
------------------------------------------------------------------------------------------------------------------

-- Goal:
--   Understand why certain records contain missing values (NULL or empty strings)
--   and decide whether these records should be kept or filtered out.

-- Purpose:
--   Ensure that further calculations (e.g., price per m²) use only reliable data.
------------------------------------------------------------------------------------------------------------------


------------------------------------------------------------
-- A) LIVING AREA (usable floor area)
------------------------------------------------------------

-- Thresholds used in this analysis:
--   • 16 m²  → minimum legal living area in the Czech Republic 
--              (anything smaller is unrealistic for a residential listing).
--   • 500 m² → upper outlier threshold; such large apartments are extremely rare
--              and often represent incorrect or misreported values.

-- These limits help filter out implausible values so they do not distort statistics.

SELECT 
  CASE 
    WHEN "data_livingArea" IS NULL OR "data_livingArea" IN ('', '.')
         THEN 'missing'                           -- no value provided

    WHEN TRY_TO_DECIMAL("data_livingArea") <= 16
         THEN 'suspiciously_small'                -- unrealistically small area

    WHEN TRY_TO_DECIMAL("data_livingArea") > 500
         THEN 'suspiciously_large'                -- unrealistically large area

    ELSE 'ok'                                     -- value appears valid
  END AS category,
  COUNT(*) AS n
FROM STG_APIFY_CLEAN_UNIQUE
GROUP BY category
ORDER BY n DESC;

-- Interpretation:
--   • missing             → listings without living area data
--   • suspiciously_small  → likely incorrect (e.g., garages, incorrect unit)
--   • suspiciously_large  → could be large houses or misreported values
--   • ok                  → typical usable living area
--
--   These categories help determine which ranges (e.g., ≤16 m² or >500 m²)
--   should be filtered out in the next cleaning steps.
------------------------------------------------------------------------------------------------------------------
------------------------------------------------------------
-- B) PRICE TOTAL (total listing price)
------------------------------------------------------------

SELECT 
  CASE 
    WHEN "data_priceTotal" IS NULL OR "data_priceTotal" IN ('', '.')
         THEN 'missing'                   -- price is not provided

    WHEN TRY_TO_DECIMAL("data_priceTotal") <= 0
         THEN 'invalid_zero_or_neg'       -- zero or negative price value

    ELSE 'ok'                             -- valid price
  END AS category,
  COUNT(*) AS n
FROM STG_APIFY_CLEAN_UNIQUE
GROUP BY category
ORDER BY n DESC;

-- Interpretation:
--   • missing             → listings with no price specified
--   • invalid_zero_or_neg → unrealistic values (e.g., 0 CZK or negative)
--   • ok                  → valid price values
--
--   If there is a large proportion of “missing”, we must decide whether to keep
--   these records for separate analysis (e.g., "price on request" listings).

------------------------------------------------------------
-- C) DISTRICT
------------------------------------------------------------

SELECT 
  CASE 
    WHEN "data_district" IS NULL OR "data_district" IN ('', '.')
         THEN 'missing'
    ELSE 'ok'
  END AS category,
  COUNT(*) AS n
FROM STG_APIFY_CLEAN_UNIQUE
GROUP BY category
ORDER BY n DESC;

-- Interpretation:
--   • missing → district information is not available
--   • ok      → district value is provided
--
--   Missing district values will later be compared with GPS coordinates
--   to determine whether the regional information can be reconstructed
--   (e.g., assigning a listing to the correct region).
------------------------------------------------------------
------------------------------------------------------------
-- D) GPS COORDINATES
------------------------------------------------------------

SELECT 
  CASE 
    WHEN "data_gpsCoord_lat" IS NULL 
      OR "data_gpsCoord_lon" IS NULL
         THEN 'missing'                      -- no coordinates provided

    WHEN TRY_TO_DOUBLE("data_gpsCoord_lat") BETWEEN 48.5 AND 51.1
     AND TRY_TO_DOUBLE("data_gpsCoord_lon") BETWEEN 12.0 AND 18.9
         THEN 'valid_cz_range'               -- coordinates fall within the Czech Republic

    ELSE 'out_of_range'                      -- coordinates outside CZ or incorrect format
  END AS category,
  COUNT(*) AS n
FROM STG_APIFY_CLEAN_UNIQUE
GROUP BY category
ORDER BY n DESC;

-- Interpretation:
--   • missing         → GPS data unavailable → cannot assign region by location
--   • valid_cz_range  → coordinates fall within CZ (valid)
--   • out_of_range    → values outside expected geographic boundaries
--                        (likely incorrect or foreign listings)
------------------------------------------------------------

-- Summary:
--   This step identifies where essential geographic or pricing data is missing
--   and helps determine whether these records can be kept, corrected,
--   or should be filtered out.
--
--   The results guide the next step, where specific filtering rules
--   (e.g., removing listings <16 m² or those without GPS/district)
--   will be applied.
------------------------------------------------------------------------------------------------------------------


------------------------------------------------------------------------------------------------------------------
-- STEP 7: Summary of missing or suspicious values
------------------------------------------------------------------------------------------------------------------

-- Goal:
--   Determine where and why data is missing, and evaluate whether those records
--   are still useful for further analysis.
--   This step helps define filtering rules (e.g., removing listings <16 m²).

-- Findings:
--   • Living area (data_livingArea):
--       - 291,624 records (~14%) are missing or contain unrealistic values.
--       - 9,799 records have suspiciously small area (<16 m²).
--       - 19,247 records have suspiciously large area (>500 m²).
--       → Approximately 85% of records appear valid.

--   • Total price (data_priceTotal):
--       - 73,573 records (~3.6%) are missing (e.g., “price upon request”).
------------------------------------------------------------------------------------------------------------------
-- STEP 8: Filter out records without geographic information (GPS or district)
--         and without valid living area
------------------------------------------------------------------------------------------------------------------

-- Goal:
--   Remove listings that do not contain at least some form of geographic information
--   (valid GPS coordinates OR a provided district), and listings that lack a valid
--   living area measurement.

-- Purpose:
--   Without geographic data, properties cannot be assigned to regions.
--   Without a usable living area, price-per-m² cannot be calculated.
--
-- Filtering rules applied:
--   A) A record must have either:
--        • valid GPS coordinates (both latitude and longitude), OR
--        • a non-empty district value.
--
--   B) Living area must fall within realistic bounds:
--        • minimum 16 m²
--        • maximum 500 m²
------------------------------------------------------------------------------------------------------------------

CREATE OR REPLACE TEMPORARY TABLE STG_APIFY_GEOAREA AS
SELECT *
FROM STG_APIFY_CLEAN_UNIQUE
WHERE
    (
        -- A1) Valid GPS coordinates (lat AND lon present and not empty)
        (
            "data_gpsCoord_lat" IS NOT NULL AND "data_gpsCoord_lat" NOT IN ('', '.')
            AND
            "data_gpsCoord_lon" IS NOT NULL AND "data_gpsCoord_lon" NOT IN ('', '.')
        )
        OR
        -- A2) Or district is provided
        (
            "data_district" IS NOT NULL AND "data_district" NOT IN ('', '.')
        )
    )
    -- B) Valid living area (16–500 m²)
    AND TRY_TO_DECIMAL("data_livingArea") BETWEEN 16 AND 500;

------------------------------------------------------------------------------------------------------------------
-- STEP 8: Verify number of records after filtering
------------------------------------------------------------------------------------------------------------------

SELECT COUNT(*) AS total_rows_after_cut
FROM STG_APIFY_GEOAREA;

-- Interpretation:
--   • Only listings that contain either GPS coordinates or a district,
--     and have a living area greater than 16 m², are retained.
--   • These records form the minimum required dataset for further analysis
--     (e.g., price per m², assigning properties to regions).
------------------------------------------------------------------------------------------------------------------


-- Optional preview of the filtered dataset
SELECT *
FROM STG_APIFY_GEOAREA;

------------------------------------------------------------------------------------------------------------------
-- STEP 8: Save the filtered dataset as a permanent table
------------------------------------------------------------------------------------------------------------------

-- Goal:
--   Create a permanent table (APIFY_GEOAREA_FINAL) from the temporary table
--   STG_APIFY_GEOAREA for further processing and team collaboration.

-- Purpose:
--   The final table contains only records with usable geographic information
--   (GPS or district) and realistic living area (16–500 m²).
--   It forms the foundation for all subsequent steps, such as:
--     • calculating price per m²,
--     • assigning listings to regions,
--     • distinguishing between rental and sale data.

CREATE OR REPLACE TABLE APIFY_GEOAREA_FINAL AS
SELECT *
FROM STG_APIFY_GEOAREA;

------------------------------------------------------------------------------------------------------------------
-- Verification: Ensure that the permanent table was created correctly
------------------------------------------------------------------------------------------------------------------

--   • COUNT(*)           → total number of rows
--   • COUNT(DISTINCT id) → number of unique listings
--   These checks confirm that no duplicates were reintroduced
--   and that the dataset matches the expected size.

SELECT 
    COUNT(*) AS total_rows,
    COUNT(DISTINCT "id") AS unique_listings
FROM APIFY_GEOAREA_FINAL;
------------------------------------------------------------------------------------------------------------------

------------------------------------------------------------------------------------------------------------------
-- STEP 9: Reduce the dataset to essential attributes
------------------------------------------------------------------------------------------------------------------

-- Goal:
--   Create a simplified table for further price-per-square-meter analysis.

-- Purpose:
--   Remove non-essential columns (e.g., images, descriptions, full GPS data, URLs)
--   and keep only the key attributes needed for analytical tasks.

-- Output:
--   The table APIFY_CLEAN_REDUCED will contain only the core fields.
------------------------------------------------------------------------------------------------------------------

CREATE OR REPLACE TABLE APIFY_CLEAN_REDUCED AS
SELECT
    "id",
    "data_offerType",      -- offer type (rent / sale)
    "data_type",           -- property type (apartment / house)
    "data_priceTotal",     -- total listing price
    "data_livingArea",     -- usable living area in m²
    "data_city",           -- city
    "data_district"        -- district
FROM APIFY_GEOAREA_FINAL;

------------------------------------------------------------------------------------------------------------------
-- Verification: number of rows and unique IDs
------------------------------------------------------------------------------------------------------------------

SELECT 
    COUNT(*) AS total_rows,
    COUNT(DISTINCT "id") AS unique_ids
FROM APIFY_CLEAN_REDUCED;

-- Show table structure
SHOW COLUMNS IN APIFY_CLEAN_REDUCED;

-- Interpretation:
--   The number of rows should match the filtered dataset (~1,752,062 rows).
--   This confirms that the table was reduced correctly and only relevant
--   columns were preserved for price-per-m² analysis.
------------------------------------------------------------------------------------------------------------------

------------------------------------------------------------------------------------------------------------------
-- STEP 10: Logical data cleaning
------------------------------------------------------------------------------------------------------------------

-- Goal:
--   Apply basic sanity checks to remove records with unrealistic or invalid values.

-- Logic applied:
--   • Price must be present and greater than 0.
--   • Living area must be valid and fall within 16–500 m².
--   • District must not contain numeric-only values (filters out corrupted records).
------------------------------------------------------------------------------------------------------------------

CREATE OR REPLACE TABLE APIFY_CLEAN_LOGICAL AS
SELECT *
FROM APIFY_CLEAN_REDUCED
WHERE 
    -- Valid price
    TRY_TO_NUMBER("data_priceTotal") IS NOT NULL
    AND TRY_TO_NUMBER("data_priceTotal") > 0

    -- Valid living area (16–500 m²)
    AND TRY_TO_NUMBER("data_livingArea") IS NOT NULL
    AND TRY_TO_NUMBER("data_livingArea") BETWEEN 16 AND 500

    -- Valid district (no numeric-only values)
    AND ("data_district" IS NULL OR "data_district" NOT RLIKE '^[0-9]+$');

------------------------------------------------------------------------------------------------------------------
-- Verification: number of rows and unique IDs after cleaning
------------------------------------------------------------------------------------------------------------------

SELECT 
    COUNT(*) AS total_rows_after_cleaning,
    COUNT(DISTINCT "id") AS unique_ids_after_cleaning
FROM APIFY_CLEAN_LOGICAL;

------------------------------------------------------------------------------------------------------------------
-- STEP 10B: Random sample to manually inspect the cleaned dataset
------------------------------------------------------------------------------------------------------------------

SELECT 
    "id",
    "data_city",
    "data_district",
    "data_offerType",
    "data_type",
    "data_priceTotal",
    "data_livingArea"
FROM APIFY_CLEAN_LOGICAL
SAMPLE (0.001);

------------------------------------------------------------------------------------------------------------------
-- Check extreme values by computing price per m²
------------------------------------------------------------------------------------------------------------------

SELECT 
    MIN(TRY_TO_NUMBER("data_priceTotal") 
        / NULLIF(TRY_TO_NUMBER("data_livingArea"), 0)) AS min_price_per_m2,
    MAX(TRY_TO_NUMBER("data_priceTotal") 
        / NULLIF(TRY_TO_NUMBER("data_livingArea"), 0)) AS max_price_per_m2,
    AVG(TRY_TO_NUMBER("data_priceTotal") 
        / NULLIF(TRY_TO_NUMBER("data_livingArea"), 0)) AS avg_price_per_m2
FROM APIFY_CLEAN_LOGICAL;

------------------------------------------------------------------------------------------------------------------
-- Check districts for incorrect or empty values
------------------------------------------------------------------------------------------------------------------

SELECT DISTINCT "data_district"
FROM APIFY_CLEAN_LOGICAL
WHERE "data_district" RLIKE '^[0-9]' 
   OR "data_district" IN ('', '.', ' ')
LIMIT 20;

------------------------------------------------------------------------------------------------------------------
-- Additional district validation
------------------------------------------------------------------------------------------------------------------

SELECT DISTINCT "data_district"
FROM APIFY_CLEAN_LOGICAL
WHERE "data_district" RLIKE '^[0-9]' 
   OR "data_district" IN ('', '.', ' ')
LIMIT 20;
------------------------------------------------------------------------------------------------------------------

------------------------------------------------------------------------------------------------------------------
-- Overview: count records by offer type (rent vs. sale)
------------------------------------------------------------------------------------------------------------------

SELECT 
    "data_offerType",
    COUNT(*) AS count_rows
FROM APIFY_CLEAN_LOGICAL
GROUP BY "data_offerType";

------------------------------------------------------------------------------------------------------------------
-- STEP 11: Validate price-per-square-meter values and remove extremes
------------------------------------------------------------------------------------------------------------------

-- Goal:
--   Verify that price-per-m² values fall within realistic ranges and remove
--   listings that contain implausible values caused by incorrect price or area.

-- Purpose:
--   The raw dataset contains listings with unrealistic values
--   (e.g., extremely low or extremely high price per m²),
--   which would distort averages and regional comparisons.

-- Method:
--   1. Compute price_per_m2 = total_price / living_area.
--   2. Apply separate valid ranges for rental and sale listings:
--
--        RENT:  50  to 1,500   CZK/m²/month
--        SALE:  5,000 to 300,000 CZK/m²
--
--   These thresholds remove outliers while preserving realistic listings.
------------------------------------------------------------------------------------------------------------------

CREATE OR REPLACE TABLE APIFY_CLEAN_PRICE AS
SELECT
    t.*,
    TRY_TO_NUMBER("data_priceTotal") 
        / NULLIF(TRY_TO_NUMBER("data_livingArea"), 0) AS price_per_m2
FROM APIFY_CLEAN_LOGICAL t
WHERE
    TRY_TO_NUMBER("data_priceTotal") IS NOT NULL
    AND TRY_TO_NUMBER("data_livingArea") IS NOT NULL
    AND TRY_TO_NUMBER("data_livingArea") BETWEEN 16 AND 500
    AND (
          (
            "data_offerType" = 'rent'
            AND (TRY_TO_NUMBER("data_priceTotal") 
                 / NULLIF(TRY_TO_NUMBER("data_livingArea"), 0))
                    BETWEEN 50 AND 1500
          )
          OR
          (
            "data_offerType" = 'sale'
            AND (TRY_TO_NUMBER("data_priceTotal") 
                 / NULLIF(TRY_TO_NUMBER("data_livingArea"), 0))
                    BETWEEN 5000 AND 300000
          )
        );

------------------------------------------------------------------------------------------------------------------
-- Additional inspection: extreme rent price-per-m² values (sorted)
------------------------------------------------------------------------------------------------------------------

SELECT
    t.*,
    TRY_TO_NUMBER("data_priceTotal") 
        / NULLIF(TRY_TO_NUMBER("data_livingArea"), 0) AS price_per_m2
FROM APIFY_CLEAN_LOGICAL t
WHERE
    TRY_TO_NUMBER("data_priceTotal") IS NOT NULL
    AND TRY_TO_NUMBER("data_livingArea") IS NOT NULL
    AND TRY_TO_NUMBER("data_livingArea") BETWEEN 16 AND 500
    AND "data_offerType" = 'rent'
    AND (TRY_TO_NUMBER("data_priceTotal") 
         / NULLIF(TRY_TO_NUMBER("data_livingArea"), 0))
            BETWEEN 50 AND 1500
ORDER BY price_per_m2;

------------------------------------------------------------------------------------------------------------------
-- Validation: row counts and price-per-m² range after cleaning
------------------------------------------------------------------------------------------------------------------

SELECT 
    COUNT(*) AS total_rows_after_cut,
    COUNT(DISTINCT "id") AS unique_ads_after_cut
FROM APIFY_CLEAN_PRICE;

-- Price-per-m² statistics by offer type
SELECT 
    "data_offerType" AS offer_type,
    MIN(price_per_m2) AS min_price_per_m2,
    MAX(price_per_m2) AS max_price_per_m2,
    AVG(price_per_m2) AS avg_price_per_m2
FROM APIFY_CLEAN_PRICE
GROUP BY offer_type;

-- Interpretation:
--   • Price-per-m² values now fall within realistic ranges.
--   • APIFY_CLEAN_PRICE is ready for further analysis
--     (e.g., regional and yearly averages).
------------------------------------------------------------------------------------------------------------------

------------------------------------------------------------------------------------------------------------------
-- STEP 12: Compute average price per m² by region and offer type
------------------------------------------------------------------------------------------------------------------

-- Goal:
--   Create a summary table showing the average price per square meter
--   separately for rental listings (rent) and sale listings (sale),
--   and eventually also by region.

-- Purpose:
--   This table will serve as a basis for further analysis and visualization,
--   such as comparing housing prices with average wages or fertility levels
--   across different regions.

-- Method:
--   • Use the cleaned dataset APIFY_CLEAN_PRICE.
--   • Group by region (once available) and offer type.
--   • Compute the average price per m² (price_per_m2).
--   • Include the number of records for transparency.

-- Note:
--   If "data_region" has not yet been assigned (e.g., added later via district-to-region mapping),
--   the calculation will run without region-level grouping and can be repeated once regions are added.
------------------------------------------------------------------------------------------------------------------

CREATE OR REPLACE TABLE APIFY_AVG_PRICE_REGION AS
SELECT
    "data_offerType" AS offer_type,      -- rent / sale
    AVG(price_per_m2) AS avg_price_per_m2,
    COUNT(*) AS count_rows               -- sample size
FROM APIFY_CLEAN_PRICE
GROUP BY "data_offerType";

------------------------------------------------------------------------------------------------------------------
-- Quick preview: random sample of the summary table
------------------------------------------------------------------------------------------------------------------

SELECT *
FROM APIFY_AVG_PRICE_REGION
SAMPLE (0.05);

-- Interpretation:
--   • APIFY_AVG_PRICE_REGION now contains the average price per m²
--     for rental and sale listings.
--   • Once region data is added, the table can be recalculated to produce
--     regional averages.
--   • This completes the data-cleaning workflow and transitions into
--     the analytical and visualization phase.
------------------------------------------------------------------------------------------------------------------
