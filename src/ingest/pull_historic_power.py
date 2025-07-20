#!/usr/bin/env python3
"""
Back-fill Growatt 5-minute power data into MongoDB.
"""
import datetime as dt
import os
import time
import sys
from dotenv import load_dotenv
from pymongo import MongoClient, errors, UpdateOne
import growattServer

load_dotenv()
plant_id = os.getenv("GROWATT_PLANT_ID") or sys.exit(
    "❌ GROWATT_PLANT_ID missing in .env"
)
api_token = os.getenv("GROWATT_TOKEN") or sys.exit(
    "❌ GROWATT_TOKEN missing in .env"
)
mongo_uri = os.getenv("MONGODB_URI") or sys.exit(
    "❌ MONGODB_URI missing in .env"
)

api = growattServer.OpenApiV1(token=api_token)
db = MongoClient(mongo_uri)["solar_data"]
pwr = db["power_readings"]
meta = db["ingest_meta"]

# ---------- Get starting cursor ----------
meta_doc = meta.find_one({"plant_id": int(plant_id)})
cursor = (
    dt.date.fromisoformat(meta_doc["last_date"])
    if meta_doc
    else dt.date.today()
)

print(f"⬇️  Starting pull at {cursor.isoformat()} for plant {plant_id}")

while True:
    try:
        payload = api.plant_power_overview(plant_id=plant_id, day=cursor)
    except Exception as e:
        print(f"⚠️  API error on {cursor}: {e} — sleeping & retrying")
        time.sleep(5)
        continue

    records = payload.get("powers", [])
    # If service returns irregular structure fail fast
    if len(records) == 0:
        print(f"❗ Empty payload on {cursor}. Exiting.")
        break

    # Stop criterion: no valid power values
    if all(r["power"] in (None, 0) for r in records):
        print(
            f"🛑 Reached a day without data ({cursor}); historic window exhausted."
        )
        break

    # Prepare Mongo docs
    docs = [
        {
            "plant_id": int(plant_id),
            "timestamp": dt.datetime.fromisoformat(r["time"]),
            "power_w": (r["power"] or 0.0),
        }
        for r in records
        if r["time"]
    ]

    # Insert only if not already present (ensures idempotence)
    # Using ordered=False keeps bulk going on duplicates
    try:
        pwr.insert_many(docs, ordered=False)
        ok = True
    except errors.BulkWriteError as bwe:
        # Ignore duplicate key errors, report others
        dups = sum(
            1 for err in bwe.details["writeErrors"] if err["code"] == 11000
        )
        if dups != len(bwe.details["writeErrors"]):
            raise  # real failure
        ok = True

    if ok:
        meta.update_one(
            {"plant_id": int(plant_id)},
            {
                "$set": {
                    "last_date": cursor.isoformat(),
                    "updated_at": dt.datetime.utcnow(),
                }
            },
            upsert=True,
        )
        print(f"✅ {cursor} ({len(docs)} pts) saved.")
        cursor -= dt.timedelta(days=1)
        time.sleep(5)
