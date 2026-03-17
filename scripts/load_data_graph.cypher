MATCH (n) DETACH DELETE n;

CREATE INDEX ON :Client(client_id);
CREATE INDEX ON :User(user_id);
CREATE INDEX ON :Campaign(id);
CREATE INDEX ON :Message(campaign_id);
CREATE INDEX ON :Event(user_id);

LOAD CSV FROM 'file:///import/client_first_purchase_date.csv' WITH HEADER AS row
CREATE (c:Client {
    client_id: ToInteger(row.client_id),
    first_purchase_date: row.first_purchase_date,
    user_id: ToInteger(row.user_id),
    user_device_id: ToInteger(row.user_device_id)
});

LOAD CSV FROM 'file:///import/campaigns.csv' WITH HEADER AS row
CREATE (camp:Campaign {
    id: ToInteger(row.id),
    campaign_type: row.campaign_type,
    channel: row.channel,
    topic: row.topic,
    started_at: row.started_at,
    finished_at: row.finished_at,
    total_count: ToFloat(row.total_count),
    warmup_mode: CASE WHEN row.warmup_mode = 'True' THEN true ELSE false END,
    subject_length: ToFloat(row.subject_length),
    subject_with_personalization: CASE WHEN row.subject_with_personalization = 'True' THEN true ELSE false END,
    subject_with_deadline: CASE WHEN row.subject_with_deadline = 'True' THEN true ELSE false END,
    subject_with_emoji: CASE WHEN row.subject_with_emoji = 'True' THEN true ELSE false END,
    subject_with_bonuses: CASE WHEN row.subject_with_bonuses = 'True' THEN true ELSE false END,
    subject_with_discount: CASE WHEN row.subject_with_discount = 'True' THEN true ELSE false END,
    subject_with_saleout: CASE WHEN row.subject_with_saleout = 'True' THEN true ELSE false END,
    is_test: CASE WHEN row.is_test = 'True' THEN true ELSE false END
});

LOAD CSV FROM 'file:///import/messages.csv' WITH HEADER AS row
CREATE (m:Message {
    id: ToInteger(row.id),
    message_id: row.message_id,
    campaign_id: ToInteger(row.campaign_id),
    message_type: row.message_type,
    client_id: ToInteger(row.client_id),
    channel: row.channel,
    email_provider: row.email_provider,
    stream: row.stream,
    date: row.date,
    sent_at: row.sent_at,
    is_opened: CASE WHEN row.is_opened = 'True' THEN true ELSE false END,
    is_clicked: CASE WHEN row.is_clicked = 'True' THEN true ELSE false END,
    is_unsubscribed: CASE WHEN row.is_unsubscribed = 'True' THEN true ELSE false END,
    is_hard_bounced: CASE WHEN row.is_hard_bounced = 'True' THEN true ELSE false END,
    is_soft_bounced: CASE WHEN row.is_soft_bounced = 'True' THEN true ELSE false END,
    is_complained: CASE WHEN row.is_complained = 'True' THEN true ELSE false END,
    is_blocked: CASE WHEN row.is_blocked = 'True' THEN true ELSE false END,
    is_purchased: CASE WHEN row.is_purchased = 'True' THEN true ELSE false END,
    created_at: row.created_at,
    updated_at: row.updated_at,
    user_device_id: ToInteger(row.user_device_id),
    user_id: ToInteger(row.user_id)
});

LOAD CSV FROM 'file:///import/events.csv' WITH HEADER AS row
CREATE (e:Event {
    event_time: row.event_time,
    event_type: row.event_type,
    product_id: ToInteger(row.product_id),
    category_id: ToInteger(row.category_id),
    category_code: row.category_code,
    brand: row.brand,
    price: ToFloat(row.price),
    user_id: ToInteger(row.user_id),
    user_session: row.user_session
});

LOAD CSV FROM 'file:///import/friends.csv' WITH HEADER AS row
MATCH (u1:User {user_id: ToInteger(row.friend1)})
MATCH (u2:User {user_id: ToInteger(row.friend2)})
CREATE (u1)-[:FRIENDS_WITH]->(u2);

MATCH (m:Message), (c:Campaign)
WHERE m.campaign_id = c.id
CREATE (m)-[:BELONGS_TO]->(c);

MATCH (m:Message), (cl:Client)
WHERE m.client_id = cl.client_id
CREATE (m)-[:SENT_TO]->(cl);
