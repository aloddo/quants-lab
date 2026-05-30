#!/usr/bin/env python3
"""
One-time script: deduplicate bybit_open_interest and add unique index.

The bybit_open_interest collection was missing a unique index on (pair, timestamp_utc),
resulting in 16,646+ duplicate entries. This script:
1. Finds duplicates (same pair + timestamp_utc)
2. Keeps the latest _id per group, deletes the rest
3. Creates a unique index to prevent future duplicates

Run:
    MONGO_URI=mongodb://localhost:27017/quants_lab \
    /Users/hermes/miniforge3/envs/quants-lab/bin/python scripts/fix_oi_duplicates.py
"""

import os
from pymongo import MongoClient, ASCENDING

MONGO_URI = os.environ.get("MONGO_URI", "mongodb://localhost:27017/quants_lab")
DB_NAME = MONGO_URI.rsplit("/", 1)[-1] or "quants_lab"

client = MongoClient(MONGO_URI)
db = client[DB_NAME]
coll = db["bybit_open_interest"]

print(f"bybit_open_interest: {coll.estimated_document_count():,} docs")

# Step 1: Find duplicate groups
print("Finding duplicates...")
pipeline = [
    {"$group": {
        "_id": {"pair": "$pair", "timestamp_utc": "$timestamp_utc"},
        "count": {"$sum": 1},
        "ids": {"$push": "$_id"},
    }},
    {"$match": {"count": {"$gt": 1}}},
]

duplicates = list(coll.aggregate(pipeline, allowDiskUse=True))
print(f"Found {len(duplicates):,} duplicate groups")

# Step 2: Delete duplicates (keep last _id per group)
total_deleted = 0
for group in duplicates:
    ids_to_delete = group["ids"][:-1]  # Keep last, delete rest
    result = coll.delete_many({"_id": {"$in": ids_to_delete}})
    total_deleted += result.deleted_count

print(f"Deleted {total_deleted:,} duplicate documents")
print(f"Remaining: {coll.estimated_document_count():,} docs")

# Step 3: Create unique index
print("Creating unique index on (pair, timestamp_utc)...")
try:
    coll.create_index(
        [("pair", ASCENDING), ("timestamp_utc", ASCENDING)],
        unique=True,
        background=True,
        name="pair_1_timestamp_utc_1_unique",
    )
    print("Unique index created successfully")
except Exception as e:
    print(f"Error creating index: {e}")
    print("There may still be duplicates. Re-run the dedup step.")

# Step 4: Verify
indexes = coll.index_information()
for name, info in indexes.items():
    print(f"  Index: {name} — unique={info.get('unique', False)}, keys={info['key']}")

client.close()
print("Done.")
