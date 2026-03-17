MATCH (c:Campaign)<-[:BELONGS_TO]-(m:Message)
WHERE m.is_purchased = true
WITH c, COUNT(DISTINCT m.client_id) AS clients_purchased
MATCH (c:Campaign)<-[:BELONGS_TO]-(m:Message)
WITH c, clients_purchased, COUNT(DISTINCT m.client_id) AS clients_reached
RETURN 
    c.id AS campaign_id,
    c.campaign_type,
    c.channel,
    clients_reached,
    clients_purchased,
    ROUND(clients_purchased * 100.0 / clients_reached) AS conversion_rate
ORDER BY clients_purchased DESC
LIMIT 10;
