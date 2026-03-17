MATCH (e:Event)
WHERE e.user_id IS NOT NULL AND e.product_id IS NOT NULL
WITH 
    e.user_id AS user_id,
    e.product_id AS product_id,
    e.category_code AS category_code,
    e.brand AS brand,
    SUM(CASE 
        WHEN e.event_type = 'view' THEN 1 
        WHEN e.event_type = 'cart' THEN 2 
        WHEN e.event_type = 'purchase' THEN 5 
        ELSE 0 
    END) AS total_score
ORDER BY total_score DESC
LIMIT 100
RETURN user_id, product_id, category_code, brand, total_score;
