DROP TABLE IF EXISTS messages CASCADE;
DROP TABLE IF EXISTS campaigns CASCADE;
DROP TABLE IF EXISTS events CASCADE;
DROP TABLE IF EXISTS friends CASCADE;
DROP TABLE IF EXISTS client_first_purchase_date CASCADE;

CREATE TABLE campaigns (
    id BIGINT,
    campaign_type TEXT,
    channel TEXT,
    topic TEXT,
    started_at TIMESTAMPTZ,
    finished_at TIMESTAMPTZ,
    total_count FLOAT,
    warmup_mode BOOLEAN,
    subject_length FLOAT,
    subject_with_personalization BOOLEAN,
    subject_with_deadline BOOLEAN,
    subject_with_emoji BOOLEAN,
    subject_with_bonuses BOOLEAN,
    subject_with_discount BOOLEAN,
    subject_with_saleout BOOLEAN,
    is_test BOOLEAN
);

CREATE TABLE events (
    event_time TIMESTAMPTZ,
    event_type TEXT,
    product_id BIGINT,
    category_id BIGINT,
    category_code TEXT,
    brand TEXT,
    price FLOAT,
    user_id BIGINT,
    user_session TEXT
);

CREATE TABLE friends (
    friend1 BIGINT,
    friend2 BIGINT
);

CREATE TABLE client_first_purchase_date (
    client_id BIGINT,
    first_purchase_date DATE,
    user_id BIGINT,
    user_device_id BIGINT
);

CREATE TABLE messages (
    id BIGINT,
    message_id TEXT,
    campaign_id BIGINT,
    message_type TEXT,
    client_id BIGINT,
    channel TEXT,
    email_provider TEXT,
    stream TEXT,
    date DATE,
    sent_at TEXT,
    is_opened BOOLEAN,
    opened_first_time_at TEXT,
    opened_last_time_at TEXT,
    is_clicked BOOLEAN,
    is_unsubscribed BOOLEAN,
    is_hard_bounced BOOLEAN,
    is_soft_bounced BOOLEAN,
    is_complained BOOLEAN,
    is_blocked BOOLEAN,
    is_purchased BOOLEAN,
    created_at TEXT,
    updated_at TEXT,
    user_device_id BIGINT,
    user_id BIGINT
);

\COPY campaigns FROM 'data/cleaned/campaigns.csv' WITH (FORMAT CSV, HEADER TRUE);
\COPY events FROM 'data/cleaned/events.csv' WITH (FORMAT CSV, HEADER TRUE);
\COPY friends FROM 'data/cleaned/friends.csv' WITH (FORMAT CSV, HEADER TRUE);
\COPY client_first_purchase_date FROM 'data/cleaned/client_first_purchase_date.csv' WITH (FORMAT CSV, HEADER TRUE);
\COPY messages FROM 'data/cleaned/messages.csv' WITH (FORMAT CSV, HEADER TRUE);

CREATE INDEX idx_events_user_id ON events(user_id);
CREATE INDEX idx_messages_client_id ON messages(client_id);
CREATE INDEX idx_campaigns_id ON campaigns(id);
CREATE INDEX idx_friends_friend1 ON friends(friend1);
