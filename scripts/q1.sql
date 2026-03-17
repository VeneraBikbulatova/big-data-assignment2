WITH campaign_stats AS (
    SELECT 
        c.id AS campaign_id,
        c.campaign_type,
        c.channel,
        COUNT(DISTINCT m.client_id) AS clients_reached,
        COUNT(DISTINCT CASE WHEN m.is_purchased = true THEN m.client_id END) AS clients_purchased,
        ROUND(
            COUNT(DISTINCT CASE WHEN m.is_purchased = true THEN m.client_id END) * 100.0 / 
            NULLIF(COUNT(DISTINCT m.client_id), 0), 2
        ) AS conversion_rate
    FROM campaigns c
    JOIN messages m ON c.id = m.campaign_id
    GROUP BY c.id, c.campaign_type, c.channel
    ORDER BY clients_purchased DESC
    LIMIT 10
),
friend_suggestions AS (
    SELECT 
        m.client_id AS existing_customer,
        f.friend2 AS suggested_friend,
        c.id AS campaign_id
    FROM messages m
    JOIN friends f ON m.client_id = f.friend1
    JOIN campaigns c ON m.campaign_id = c.id
    WHERE m.is_purchased = true
    LIMIT 50
)
SELECT 
    'Campaign Performance' AS analysis_type,
    cs.campaign_id,
    cs.campaign_type,
    cs.channel,
    cs.clients_reached,
    cs.clients_purchased,
    cs.conversion_rate,
    NULL AS existing_customer,
    NULL AS suggested_friend
FROM campaign_stats cs

UNION ALL

SELECT 
    'Friend Recommendations' AS analysis_type,
    fs.campaign_id,
    NULL,
    NULL,
    NULL,
    NULL,
    NULL,
    fs.existing_customer,
    fs.suggested_friend
FROM friend_suggestions fs;
