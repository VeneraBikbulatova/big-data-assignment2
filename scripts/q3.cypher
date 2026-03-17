MATCH (e:Event)
WHERE 
    e.category_code CONTAINS 'electronics' 
    OR e.category_code CONTAINS 'clothing'
    OR e.category_code CONTAINS 'home'
WITH 
    e.product_id AS product_id,
    e.category_code AS category_code,
    e.brand AS brand,
    e.price AS price
RETURN 
    product_id,
    category_code,
    brand,
    price,
    COUNT(*) AS event_count,
    ROUND(AVG(price)) AS avg_price
ORDER BY event_count DESC
LIMIT 50;
