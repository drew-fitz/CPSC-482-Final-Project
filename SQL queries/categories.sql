CREATE OR REPLACE VIEW nike_retail.v_category_coverage AS
SELECT
    country_code,
    category,
    subcategory,
    gender_segment,
    COUNT(DISTINCT product_id)              AS unique_products,
    COUNT(*)                                AS total_skus,
    ROUND(AVG(price_usd), 2)               AS avg_price_usd,
    SUM(is_discounted)                      AS discounted_skus,
    ROUND(
        SAFE_DIVIDE(SUM(is_discounted), COUNT(*)) * 100, 1
    )                                       AS pct_discounted
FROM
    nike_retail.products_clean
WHERE
    category IS NOT NULL
GROUP BY
    country_code, category, subcategory, gender_segment
ORDER BY
    category, subcategory;
 