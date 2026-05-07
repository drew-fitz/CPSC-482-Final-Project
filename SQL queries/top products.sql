CREATE OR REPLACE VIEW nike_retail.v_top_products AS
WITH ranked AS (
    SELECT
        country_code,
        category,
        product_name,
        model_number,
        gender_segment,
        COUNT(*)                        AS sku_count,
        ROUND(AVG(price_usd), 2)        AS avg_price_usd,
        ROUND(AVG(discount_pct), 2)     AS avg_discount_pct,
        MAX(is_discounted)              AS ever_discounted,
        COUNTIF(LOWER(in_stock) = 'true')   AS in_stock_skus,
        ROW_NUMBER() OVER (
            PARTITION BY country_code, category
            ORDER BY COUNT(*) DESC
        )                               AS rank_in_category
    FROM
        nike_retail.products_clean
    WHERE
        product_name IS NOT NULL
    GROUP BY
        country_code, category,
        product_name, model_number, gender_segment
)
SELECT *
FROM ranked
WHERE rank_in_category <= 20
ORDER BY country_code, category, rank_in_category;