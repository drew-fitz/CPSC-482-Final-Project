-- ────────────────────────────────────────────────────────────
-- VIEW 1: v_price_by_country
-- Average local and USD prices by country and category.
-- Powers the global price comparison geo map and bar charts.
-- ────────────────────────────────────────────────────────────
CREATE OR REPLACE VIEW nike_retail.v_price_by_country AS
SELECT
    country_code,
    currency,
    category,
    COUNT(*)                            AS product_count,
    ROUND(AVG(price_local), 2)          AS avg_price_local,
    ROUND(MIN(price_local), 2)          AS min_price_local,
    ROUND(MAX(price_local), 2)          AS max_price_local,
    ROUND(AVG(price_usd), 2)            AS avg_price_usd,
    ROUND(MIN(price_usd), 2)            AS min_price_usd,
    ROUND(MAX(price_usd), 2)            AS max_price_usd,
    -- Price spread: how wide the range is within a country+category
    ROUND(MAX(price_usd) - MIN(price_usd), 2) AS price_spread_usd
FROM
    nike_retail.products_clean
WHERE
    price_usd IS NOT NULL
    AND price_local IS NOT NULL
    AND category IS NOT NULL
GROUP BY
    country_code, currency, category
ORDER BY
    category;