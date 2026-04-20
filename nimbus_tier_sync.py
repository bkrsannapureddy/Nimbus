"""
nimbus_tier_sync.py — Helper script for MongoDB Q1 and Q4
============================================================
Exports customer-to-tier mappings from PostgreSQL into a MongoDB
lookup collection called "customer_tiers".

This enables MongoDB aggregation pipelines to segment activity
data by subscription tier, which is stored in PostgreSQL.

Run this ONCE before executing nimbus_mongo_queries.js:
    python nimbus_tier_sync.py

This is a standard ETL pattern used when data spans multiple
database systems.
"""

import os
import psycopg2
from pymongo import MongoClient

# --- Configuration (update these for your environment) ---
PG_HOST = "localhost"
PG_DB = "nimbus_core"
PG_USER = "postgres"
PG_PASS = os.environ.get("PG_PASSWORD")  # Set PG_PASSWORD env var before running
PG_SCHEMA = "nimbus"

MONGO_URI = "mongodb://localhost:27017/"
MONGO_DB = "nimbus_events"
MONGO_COLLECTION = "customer_tiers"

# --- Extract from PostgreSQL ---
print("Connecting to PostgreSQL...")
pg_conn = psycopg2.connect(
    host=PG_HOST, database=PG_DB,
    user=PG_USER, password=PG_PASS
)
cur = pg_conn.cursor()

# Get each customer's current (latest) subscription tier
cur.execute(f"""
    SELECT DISTINCT ON (s.customer_id)
        s.customer_id,
        p.plan_tier
    FROM {PG_SCHEMA}.subscriptions s
    JOIN {PG_SCHEMA}.plans p ON s.plan_id = p.plan_id
    ORDER BY s.customer_id, s.start_date DESC
""")
rows = cur.fetchall()
print(f"  Fetched {len(rows)} customer-tier mappings from PostgreSQL")

cur.close()
pg_conn.close()

# --- Load into MongoDB ---
print("Connecting to MongoDB...")
mongo_client = MongoClient(MONGO_URI)
db = mongo_client[MONGO_DB]

# Drop and recreate for idempotency
db[MONGO_COLLECTION].drop()

docs = [{"customer_id": row[0], "plan_tier": row[1]} for row in rows]
db[MONGO_COLLECTION].insert_many(docs)

# Create index for fast lookups
db[MONGO_COLLECTION].create_index("customer_id", unique=True)

print(f"  Inserted {len(docs)} documents into {MONGO_DB}.{MONGO_COLLECTION}")
print("  Index created on customer_id")

# Verify
tier_counts = {}
for doc in db[MONGO_COLLECTION].find():
    tier = doc["plan_tier"]
    tier_counts[tier] = tier_counts.get(tier, 0) + 1
print(f"  Tier distribution: {tier_counts}")

mongo_client.close()
print("\nDone. You can now run nimbus_mongo_queries.js")
