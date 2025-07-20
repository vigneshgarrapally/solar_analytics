#!/usr/bin/env python3
"""
Back-fill Growatt DAILY energy (kWh) into MongoDB.
Runs in seven-day blocks, working backwards from today, and
records a cursor in ingest_meta so the job is resumable.
"""

import datetime as dt
import os
import sys
import time
from dotenv import load_dotenv
from pymongo import MongoClient, errors
import growattServer

# ──────────────────────────── CONFIG ──────────────────────────
load_dotenv()
PLANT_ID = int(
    os.getenv("GROWATT_PLANT_ID") or sys.exit("❌ GROWATT_PLANT_ID missing")
)
API_TOKEN = os.getenv("GROWATT_TOKEN") or sys.exit("❌ GROWATT_TOKEN missing")
MONGO_URI = os.getenv("MONGODB_URI") or sys.exit("❌ MONGODB_URI missing")
PAUSE_S = float(os.getenv("PAUSE_SECONDS", 5))

# ──────────────────────────── CLIENTS ─────────────────────────
api = growattServer.OpenApiV1(token=API_TOKEN)
db = MongoClient(MONGO_URI)["solar_data"]
ener = db["daily_energy"]
meta = db["ingest_meta"]

# ─────────────────── RESUME CURSOR (per plant & metric) ───────
meta_doc = meta.find_one({"plant_id": int(PLANT_ID), "metric": "energy"})
cursor_end = (
    dt.date.fromisoformat(meta_doc["last_date"])
    if meta_doc
    else dt.date.today() - dt.timedelta(days=1)
)

print(f"⬇️  Starting energy pull at {cursor_end} (7-day chunks)")

WEEK = dt.timedelta(days=7)

while True:
    start_date = cursor_end - (
        WEEK - dt.timedelta(days=1)
    )  # inclusive 7-day window
    try:
        payload = api.plant_energy_history(
            plant_id=PLANT_ID,
            start_date=start_date,
            end_date=cursor_end,
            time_unit="day",
            page=1,
            perpage=99,  # plenty for 7 records
        )
    except Exception as err:
        print(f"⚠️  API error {err} - retrying after pause")
        time.sleep(PAUSE_S * 3)
        continue

    rows = payload.get("energys", [])
    if not rows:
        print(
            f"❗ No data returned for {start_date} → {cursor_end}. Exiting."
        )
        break

    # Stop if the whole week is empty / zero
    if all(r["energy"] in ("", None, "0", 0, 0.0) for r in rows):
        print(
            f"🛑 First all-zero week reached ({start_date}-{cursor_end}). Done."
        )
        break

    docs = []
    for r in rows:
        try:
            day = dt.date.fromisoformat(r["date"])
            kwh = float(r["energy"])
        except (ValueError, TypeError):
            continue  # skip corrupt rows
        docs.append(
            {
                "plant_id": PLANT_ID,
                "date": dt.datetime.combine(day, dt.time.min),
                "energy_kwh": kwh,
            }
        )

    # insert with duplicate-tolerant bulk write
    try:
        ener.insert_many(docs, ordered=False)
    except errors.BulkWriteError as bwe:
        if any(e["code"] != 11000 for e in bwe.details["writeErrors"]):
            raise  # re-throw non-duplicate errors

    # update cursor
    meta.update_one(
        {"plant_id": PLANT_ID, "metric": "energy"},
        {
            "$set": {
                "last_date": start_date.isoformat(),
                "updated_at": dt.datetime.utcnow(),
            }
        },
        upsert=True,
    )

    print(f"✅ {start_date} → {cursor_end} ({len(docs)} days) saved.")
    cursor_end = start_date - dt.timedelta(days=1)  # step back a week
    time.sleep(PAUSE_S)
