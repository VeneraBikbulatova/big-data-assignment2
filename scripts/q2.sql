WITH user_product_scores AS (
    SELECT 
        user_id,
        product_id,
        category_code,
        brand,
        SUM(CASE 
            WHEN event_type = 'view' THEN 1 
            WHEN event_type = 'cart' THEN 2 
            WHEN event_type = 'purchase' THEN 5 
            ELSE 0 
        END) AS total_score
    FROM events
    WHERE user_id IS NOT NULL AND product_id IS NOT NULL
    GROUP BY user_id, product_id, category_code, brand
),
ranked AS (
    SELECT *,
        ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY total_score DESC) AS rank
    FROM user_product_scores
)
SELECT user_id, product_id, category_code, brand, total_score
FROM ranked
WHERE rank <= 5
ORDER BY user_id, rank
LIMIT 100;
