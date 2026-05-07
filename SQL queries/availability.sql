CREATE OR REPLACE VIEW nike_retail.v_availability AS
SELECT
    country_code,
    category,
    gender_segment,
    availability_level,
    COUNT(*)                                AS total_skus,
    COUNTIF(LOWER(in_stock) = 'true')       AS in_stock_count,
    COUNTIF(LOWER(in_stock) = 'false')      AS out_of_stock_count,
    ROUND(
        SAFE_DIVIDE(
            COUNTIF(LOWER(in_stock) = 'true'), COUNT(*)
        ) * 100, 1
    )                                       AS in_stock_pct,
    -- Availability level breakdown (HIGH / LOW / OOS)
    COUNTIF(UPPER(availability_level) = 'HIGH')     AS avail_high,
    COUNTIF(UPPER(availability_level) = 'LOW')      AS avail_low,
    COUNTIF(UPPER(availability_level) = 'OOS')      AS avail_oos
FROM
    nike_retail.products_clean
WHERE
    in_stock IS NOT NULL
GROUP BY
    country_code, category, gender_segment, availability_level
ORDER BY
    category, availability_level;
 
 