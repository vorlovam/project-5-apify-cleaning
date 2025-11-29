/* 
    Final SQL script used in the hackathon to clean the Apify real-estate dataset,
    normalize district names, join with the Czech administrative dataset (UZEMI),
    and compute average and median price per square meter by region and year.
*/

------------------------------------------------------------
-- STEP 1: Deduplication of advertisements
------------------------------------------------------------
WITH DEDUPLIKACE AS (
    SELECT
        "createdAt" AS DATUM,
        YEAR(TO_TIMESTAMP_NTZ("createdAt")) AS ROK,
        "id" AS ID_INZERAT,
        "data_offerType"    AS TYP_NABIDKY,
        "data_type"         AS TYP_NEMOVITOSTI,
        TRY_TO_NUMBER(NULLIF("data_priceTotal", '')) AS CENA_CELKEM,
        TRY_TO_NUMBER(NULLIF("data_livingArea", '')) AS PLOCHA,

        -- Fix known district exceptions (Praha, Ostrava)
        CASE 
            WHEN "data_district" ILIKE 'Hlavní město Praha' THEN 'Praha'
            WHEN "data_district" ILIKE 'Ostrava' THEN 'Ostrava-město'
            ELSE "data_district"
        END AS OKRES,

        -- Normalized district for reliable joining
        LOWER(
            REGEXP_REPLACE(
                REGEXP_REPLACE(
                    TRIM(
                        CASE
                            WHEN "data_district" ILIKE 'Hlavní město Praha' THEN 'Praha'
                            WHEN "data_district" ILIKE 'Ostrava' THEN 'Ostrava-město'
                            ELSE "data_district"
                        END
                    ),
                '\\s*-\\s*', '-'
                ),
            '\\s+', ' '
            )
        ) AS OKRES_NORM,

        "data_gpsCoord_lat",
        "data_gpsCoord_lon"

    FROM KBC_EUW3_2105."in.c-apify-apify-01k7vb83wnr044yxwkakahqma8"."dataset-items"
    QUALIFY ROW_NUMBER() OVER (PARTITION BY "id" ORDER BY "id") = 1
),

------------------------------------------------------------
-- STEP 2: Regions reference table (UZEMI)
------------------------------------------------------------
KRAJE AS (
    SELECT DISTINCT
        "okres_text",
        LOWER(
            REGEXP_REPLACE(
                REGEXP_REPLACE(TRIM("okres_text"), '\\s*-\\s*', '-'),
                '\\s+', ' '
            )
        ) AS OKRES_NORM,
        "kraj_text"
    FROM KBC_EUW3_2105."in.c-PROJEKT_DA_JANA"."UZEMI"
),

------------------------------------------------------------
-- STEP 3: Join and cleaning
------------------------------------------------------------
TABULKA_SKORO AS (
    SELECT
        apify.*,
        uzemi."okres_text" AS OKRES_STAT,
        uzemi.OKRES_NORM,
        uzemi."kraj_text" AS KRAJ,
        CENA_CELKEM / PLOCHA AS CENA_ZA_M
    FROM DEDUPLIKACE apify
    LEFT JOIN KRAJE uzemi
        ON LOWER(TRIM(apify.OKRES_NORM)) = uzemi.OKRES_NORM
    WHERE 
        TYP_NEMOVITOSTI IN ('apartment', 'house')
        AND TYP_NABIDKY IN ('sale','rent')
        AND TRY_TO_DECIMAL(PLOCHA) BETWEEN 16 AND 500
        AND CENA_CELKEM IS NOT NULL
        AND ("data_gpsCoord_lat" IS NOT NULL)
        AND ("data_gpsCoord_lon" IS NOT NULL)
        AND TRY_TO_DOUBLE("data_gpsCoord_lat") BETWEEN 48.5 AND 51.1
        AND TRY_TO_DOUBLE("data_gpsCoord_lon") BETWEEN 12.0 AND 18.9
)

------------------------------------------------------------
-- FINAL OUTPUT: Average and median price per m² by year and region
------------------------------------------------------------
SELECT
    ROK,
    UPPER(KRAJ) AS KRAJ,
    TYP_NABIDKY,
    TYP_NEMOVITOSTI,
    AVG(CENA_ZA_M)    AS PRUMER_CENA_ZA_M_KRAJ,
    MEDIAN(CENA_ZA_M) AS MEDIAN_CENA_ZA_M_KRAJ
FROM TABULKA_SKORO
GROUP BY ROK, KRAJ, TYP_NABIDKY, TYP_NEMOVITOSTI
ORDER BY ROK, KRAJ NULLS FIRST, TYP_NABIDKY, TYP_NEMOVITOSTI;
