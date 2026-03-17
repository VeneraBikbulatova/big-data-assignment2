SELECT 
    product_id,
    category_code,
    brand,
    price,
    COUNT(*) AS event_count,
    ROUND(AVG(price)::numeric, 2) AS avg_price
FROM events
WHERE 
    category_code ILIKE '%electronics%' 
    OR category_code ILIKE '%clothing%'
    OR category_code ILIKE '%home%'
GROUP BY product_id, category_code, brand, price
ORDER BY event_count DESC
LIMIT 50;
