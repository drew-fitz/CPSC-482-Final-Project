CREATE OR REPLACE VIEW nike_retail.v_discount_distribution AS
SELECT
    country_code,
    category,
    gender_segment,
    COUNT(*)                                        AS total_products,
    SUM(is_discounted)                              AS discounted_products,
    ROUND(
        SAFE_DIVIDE(SUM(is_discounted), COUNT(*)) * 100, 1
    )                                               AS pct_products_discounted,
    ROUND(AVG(
        CASE WHEN is_discounted = 1 THEN discount_pct END
    ), 2)                                           AS avg_discount_pct,
    ROUND(MAX(discount_pct), 2)                     AS max_discount_pct,
    -- Discount bands: count of products falling in each tier
    COUNTIF(discount_pct = 0)                       AS band_no_discount,
    COUNTIF(discount_pct > 0  AND discount_pct <= 10)  AS band_1_to_10,
    COUNTIF(discount_pct > 10 AND discount_pct <= 25)  AS band_11_to_25,
    COUNTIF(discount_pct > 25 AND discount_pct <= 50)  AS band_26_to_50,
    COUNTIF(discount_pct > 50)                         AS band_over_50
FROM
    nike_retail.products_clean
GROUP BY
    country_code, category, gender_segment
ORDER BY
    avg_discount_pct DESC;