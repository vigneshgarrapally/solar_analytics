#!/usr/bin/env python3
"""
Create MongoDB DB + collections with validators & indexes.
Run once:   python src/setup_db.py
"""

import os
from dotenv import load_dotenv
from pymongo import MongoClient, errors

load_dotenv()  # loads MONGODB_URI from .env
client = MongoClient(os.getenv("MONGODB_URI"))
db = client["solar_data"]

# ---------- 1) Power readings (time-series) ----------
try:
    db.create_collection(
        "power_readings",
        timeseries={
            "timeField": "timestamp",
            "metaField": "plant_id",
            "granularity": "minutes"
        }
    )
except errors.CollectionInvalid:
    print("power_readings already exists")

db["power_readings"].create_index(
    [("plant_id", 1), ("timestamp", 1)],
    name="plant_ts_idx"
)

# ---------- 2) Daily energy summary ----------
db.create_collection(
    "daily_energy",
    validator={
        "$jsonSchema": {
            "bsonType": "object",
            "required": ["date", "energy_kwh"],
            "properties": {
                "date":       { "bsonType": "date" },
                "energy_kwh": { "bsonType": "double", "minimum": 0 }
            }
        }
    }
)
db["daily_energy"].create_index(
    [("plant_id", 1), ("date", 1)],
    unique=True,
    name="plant_date_idx"
)

print("✅  MongoDB schema ready.")
