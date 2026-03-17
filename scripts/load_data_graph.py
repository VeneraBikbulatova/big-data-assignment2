from neo4j import GraphDatabase
import pandas as pd

URI = "bolt://localhost:7687"
AUTH = ("neo4j", "")
BATCH_SIZE = 10000

def get_driver():
    return GraphDatabase.driver(URI, auth=AUTH)

def clear_db(driver):
    with driver.session() as session:
        session.run("MATCH (n) DETACH DELETE n")

def create_indexes(driver):
    with driver.session() as session:
        session.run("CREATE INDEX ON :Client(client_id)")
        session.run("CREATE INDEX ON :User(user_id)")
        session.run("CREATE INDEX ON :Campaign(id)")
        session.run("CREATE INDEX ON :Message(campaign_id)")
        session.run("CREATE INDEX ON :Event(user_id)")

def load_clients(driver, filepath):
    df = pd.read_csv(filepath)
    with driver.session() as session:
        for i in range(0, len(df), BATCH_SIZE):
            batch = df[i:i+BATCH_SIZE]
            for _, row in batch.iterrows():
                session.run("""
                    CREATE (c:Client {
                        client_id: $client_id,
                        first_purchase_date: $first_purchase_date,
                        user_id: $user_id,
                        user_device_id: $user_device_id
                    })
                """, {
                    "client_id": int(row["client_id"]) if pd.notna(row["client_id"]) else None,
                    "first_purchase_date": str(row["first_purchase_date"]) if pd.notna(row["first_purchase_date"]) else None,
                    "user_id": int(row["user_id"]) if pd.notna(row["user_id"]) else None,
                    "user_device_id": int(row["user_device_id"]) if pd.notna(row["user_device_id"]) else None
                })
    print(f"Loaded {len(df)} clients")

def load_campaigns(driver, filepath):
    df = pd.read_csv(filepath)
    with driver.session() as session:
        for i in range(0, len(df), BATCH_SIZE):
            batch = df[i:i+BATCH_SIZE]
            for _, row in batch.iterrows():
                session.run("""
                    CREATE (c:Campaign {
                        id: $id,
                        campaign_type: $campaign_type,
                        channel: $channel,
                        topic: $topic,
                        started_at: $started_at,
                        finished_at: $finished_at,
                        total_count: $total_count,
                        warmup_mode: $warmup_mode,
                        subject_length: $subject_length,
                        subject_with_personalization: $subject_with_personalization,
                        subject_with_deadline: $subject_with_deadline,
                        subject_with_emoji: $subject_with_emoji,
                        subject_with_bonuses: $subject_with_bonuses,
                        subject_with_discount: $subject_with_discount,
                        subject_with_saleout: $subject_with_saleout,
                        is_test: $is_test
                    })
                """, {
                    "id": int(row["id"]) if pd.notna(row["id"]) else None,
                    "campaign_type": str(row["campaign_type"]) if pd.notna(row["campaign_type"]) else None,
                    "channel": str(row["channel"]) if pd.notna(row["channel"]) else None,
                    "topic": str(row["topic"]) if pd.notna(row["topic"]) else None,
                    "started_at": str(row["started_at"]) if pd.notna(row["started_at"]) else None,
                    "finished_at": str(row["finished_at"]) if pd.notna(row["finished_at"]) else None,
                    "total_count": float(row["total_count"]) if pd.notna(row["total_count"]) else None,
                    "warmup_mode": str(row["warmup_mode"]).strip().lower() == "t",
                    "subject_length": float(row["subject_length"]) if pd.notna(row["subject_length"]) else None,
                    "subject_with_personalization": str(row["subject_with_personalization"]).strip().lower() == "t",
                    "subject_with_deadline": str(row["subject_with_deadline"]).strip().lower() == "t",
                    "subject_with_emoji": str(row["subject_with_emoji"]).strip().lower() == "t",
                    "subject_with_bonuses": str(row["subject_with_bonuses"]).strip().lower() == "t",
                    "subject_with_discount": str(row["subject_with_discount"]).strip().lower() == "t",
                    "subject_with_saleout": str(row["subject_with_saleout"]).strip().lower() == "t",
                    "is_test": str(row["is_test"]).strip().lower() == "t"
                })
    print(f"Loaded {len(df)} campaigns")

def load_messages(driver, filepath):
    df = pd.read_csv(filepath)
    with driver.session() as session:
        for i in range(0, len(df), BATCH_SIZE):
            batch = df[i:i+BATCH_SIZE]
            for _, row in batch.iterrows():
                session.run("""
                    CREATE (m:Message {
                        id: $id,
                        message_id: $message_id,
                        campaign_id: $campaign_id,
                        message_type: $message_type,
                        client_id: $client_id,
                        channel: $channel,
                        email_provider: $email_provider,
                        stream: $stream,
                        date: $date,
                        sent_at: $sent_at,
                        is_opened: $is_opened,
                        is_clicked: $is_clicked,
                        is_unsubscribed: $is_unsubscribed,
                        is_hard_bounced: $is_hard_bounced,
                        is_soft_bounced: $is_soft_bounced,
                        is_complained: $is_complained,
                        is_blocked: $is_blocked,
                        is_purchased: $is_purchased,
                        created_at: $created_at,
                        updated_at: $updated_at,
                        user_device_id: $user_device_id,
                        user_id: $user_id
                    })
                """, {
                    "id": int(row["id"]) if pd.notna(row["id"]) else None,
                    "message_id": str(row["message_id"]) if pd.notna(row["message_id"]) else None,
                    "campaign_id": int(row["campaign_id"]) if pd.notna(row["campaign_id"]) else None,
                    "message_type": str(row["message_type"]) if pd.notna(row["message_type"]) else None,
                    "client_id": int(row["client_id"]) if pd.notna(row["client_id"]) else None,
                    "channel": str(row["channel"]) if pd.notna(row["channel"]) else None,
                    "email_provider": str(row["email_provider"]) if pd.notna(row["email_provider"]) else None,
                    "stream": str(row["stream"]) if pd.notna(row["stream"]) else None,
                    "date": str(row["date"]) if pd.notna(row["date"]) else None,
                    "sent_at": str(row["sent_at"]) if pd.notna(row["sent_at"]) else None,
                    "is_opened": str(row["is_opened"]).strip().lower() == "t",
                    "is_clicked": str(row["is_clicked"]).strip().lower() == "t",
                    "is_unsubscribed": str(row["is_unsubscribed"]).strip().lower() == "t",
                    "is_hard_bounced": str(row["is_hard_bounced"]).strip().lower() == "t",
                    "is_soft_bounced": str(row["is_soft_bounced"]).strip().lower() == "t",
                    "is_complained": str(row["is_complained"]).strip().lower() == "t",
                    "is_blocked": str(row["is_blocked"]).strip().lower() == "t",
                    "is_purchased": str(row["is_purchased"]).strip().lower() == "t",
                    "created_at": str(row["created_at"]) if pd.notna(row["created_at"]) else None,
                    "updated_at": str(row["updated_at"]) if pd.notna(row["updated_at"]) else None,
                    "user_device_id": int(row["user_device_id"]) if pd.notna(row["user_device_id"]) else None,
                    "user_id": int(row["user_id"]) if pd.notna(row["user_id"]) else None
                })
    print(f"Loaded {len(df)} messages")

def load_events(driver, filepath):
    df = pd.read_csv(filepath)
    with driver.session() as session:
        for i in range(0, len(df), BATCH_SIZE):
            batch = df[i:i+BATCH_SIZE]
            for _, row in batch.iterrows():
                session.run("""
                    CREATE (e:Event {
                        event_time: $event_time,
                        event_type: $event_type,
                        product_id: $product_id,
                        category_id: $category_id,
                        category_code: $category_code,
                        brand: $brand,
                        price: $price,
                        user_id: $user_id,
                        user_session: $user_session
                    })
                """, {
                    "event_time": str(row["event_time"]) if pd.notna(row["event_time"]) else None,
                    "event_type": str(row["event_type"]) if pd.notna(row["event_type"]) else None,
                    "product_id": int(row["product_id"]) if pd.notna(row["product_id"]) else None,
                    "category_id": int(row["category_id"]) if pd.notna(row["category_id"]) else None,
                    "category_code": str(row["category_code"]) if pd.notna(row["category_code"]) else None,
                    "brand": str(row["brand"]) if pd.notna(row["brand"]) else None,
                    "price": float(row["price"]) if pd.notna(row["price"]) else None,
                    "user_id": int(row["user_id"]) if pd.notna(row["user_id"]) else None,
                    "user_session": str(row["user_session"]) if pd.notna(row["user_session"]) else None
                })
    print(f"Loaded {len(df)} events")

def load_friends(driver, filepath):
    df = pd.read_csv(filepath)
    with driver.session() as session:
        for i in range(0, len(df), BATCH_SIZE):
            batch = df[i:i+BATCH_SIZE]
            for _, row in batch.iterrows():
                session.run("""
                    MERGE (u1:User {user_id: $f1})
                    MERGE (u2:User {user_id: $f2})
                    MERGE (u1)-[:FRIENDS_WITH]->(u2)
                """, {
                    "f1": int(row["friend1"]) if pd.notna(row["friend1"]) else None,
                    "f2": int(row["friend2"]) if pd.notna(row["friend2"]) else None
                })
    print(f"Loaded {len(df)} friend relationships")

def create_relationships(driver):
    with driver.session() as session:
        session.run("""
            MATCH (m:Message), (c:Campaign)
            WHERE m.campaign_id = c.id AND c.id IS NOT NULL
            MERGE (m)-[:BELONGS_TO]->(c)
        """)
        session.run("""
            MATCH (m:Message), (cl:Client)
            WHERE m.client_id = cl.client_id AND cl.client_id IS NOT NULL
            MERGE (m)-[:SENT_TO]->(cl)
        """)
    print("Relationships created")

def main():
    driver = get_driver()
    data_dir = "data/cleaned"
    
    clear_db(driver)
    create_indexes(driver)
    
    load_clients(driver, f"{data_dir}/client_first_purchase_date.csv")
    load_campaigns(driver, f"{data_dir}/campaigns.csv")
    load_messages(driver, f"{data_dir}/messages.csv")
    load_events(driver, f"{data_dir}/events.csv")
    load_friends(driver, f"{data_dir}/friends.csv")
    create_relationships(driver)
    
    driver.close()
    print("Memgraph load complete")

if __name__ == "__main__":
    main()
