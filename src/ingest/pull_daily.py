"""
Does the following:
1. Given a date pulls the daily power and energy data for a plant.
2. Calculates the daily energy from the power data.
3. Asserts that the daily energy matches the energy data from the API.
4. Inserts the data into MongoDB collections for power and energy.
"""

import datetime as dt
import os
import sys
import time
from dotenv import load_dotenv
from pymongo import MongoClient, errors
import growattServer
import traceback
from zoneinfo import ZoneInfo  # Import ZoneInfo for timezone handling

# CONFIG
load_dotenv()
PLANT_ID = int(
    os.getenv("GROWATT_PLANT_ID") or sys.exit("❌ GROWATT_PLANT_ID missing")
)
API_TOKEN = os.getenv("GROWATT_TOKEN") or sys.exit("❌ GROWATT_TOKEN missing")
MONGO_URI = os.getenv("MONGODB_URI") or sys.exit("❌ MONGODB_URI missing")
PAUSE_S = float(os.getenv("PAUSE_SECONDS", 5))

# Define the Indian Standard Timezone
IST = ZoneInfo("Asia/Kolkata")

# CLIENTS
api = growattServer.OpenApiV1(token=API_TOKEN)
# The `tz_aware=True` argument ensures that PyMongo reads data back from MongoDB as timezone-aware UTC datetimes.
db = MongoClient(MONGO_URI, tz_aware=True)["solar_data_v2"]
pwr = db["power_readings"]
ener = db["daily_energy"]


def pull_power_data(date: dt.date):
    """Pulls daily power data for a given date."""
    try:
        print(f"⬇️  Pulling power data for {date.isoformat()}...")
        payload = api.plant_power_overview(
            plant_id=PLANT_ID,
            day=date,
        )
        return payload.get("powers", [])
    except Exception as err:
        print(f"⚠️  Error pulling power data for {date}: {err}")
        time.sleep(PAUSE_S * 3)
        return None


def pull_energy_data(date: dt.date):
    """Pulls daily energy data for a given date."""
    try:
        print(f"⬇️  Pulling energy data for {date.isoformat()}...")
        payload = api.plant_energy_history(
            plant_id=PLANT_ID,
            start_date=date,
            end_date=date,
            time_unit="day",
            page=1,
            perpage=99,
        )
        return payload.get("energys", [])
    except Exception as err:
        print(f"⚠️  Error pulling energy data for {date}: {err}")
        return None


def pull_daily_data(date: dt.date):
    """Pulls daily power and energy data for a given date."""
    try:
        print(f"⬇️  Pulling data for {date.isoformat()}...")

        # Pull energy data
        energy_payload = pull_energy_data(date)
        energy = validate_and_upsert_energy_data(energy_payload, date)

        time.sleep(PAUSE_S)  # Respect API rate limits

        # Pull power data
        power_payload = pull_power_data(date)
        validate_and_upsert_power_data(
            power_payload, date, energy=energy
        )  # optionally send energy data to cross verify the math

    except Exception as err:
        print(f"⚠️  Error pulling daily data for {date}: {err}")
        time.sleep(PAUSE_S * 3)
        traceback.print_exc()


def validate_and_upsert_energy_data(energy_data, date):
    """
    Given payload and dates, does the following:
    1. Verifies whether data is valid.
    2. Check if the energy data matches the date.
    3. Upserts the data into MongoDB, converting the date to UTC.

    Args:
        energy_payload (list): The energy data payload from the API.
        date (dt.date): The date for which the data is being validated.
    """
    if not energy_data:
        print(f"❗ No energy data found for {date.isoformat()}. Skipping.")
        return None

    if len(energy_data) != 1:
        print(
            f"❗ Multiple energy records found for {date.isoformat()}. Skipping."
        )
        return None

    try:
        energy_record = energy_data[0]
        day = dt.date.fromisoformat(energy_record["date"])
        kwh = float(energy_record["energy"])
    except (ValueError, TypeError) as e:
        print(
            f"❗ Invalid energy data for {date.isoformat()}: {e}. Skipping."
        )
        return None

    if day != date:
        print(
            f"❗ Energy data date mismatch for {date.isoformat()}: {day} != {date}. Skipping."
        )
        return None

    if kwh < 0:
        print(
            f"❗ Negative energy value for {date.isoformat()}: {kwh}. Skipping."
        )
        return None

    # **TIMEZONE CHANGE**: Create a timezone-aware datetime object for the start of the day in IST.
    # PyMongo will automatically convert this to UTC upon insertion.
    date_aware = dt.datetime.combine(day, dt.time.min, tzinfo=IST)

    # Prepare MongoDB document
    doc = {
        "plant_id": PLANT_ID,
        "date": date_aware,
        "energy_kwh": kwh,
    }

    # Upsert into MongoDB
    try:
        ener.update_one(
            {"plant_id": PLANT_ID, "date": doc["date"]},
            {"$set": doc},
            upsert=True,
        )
        print(f"✅ Upserted energy data for {date.isoformat()}: {kwh} kWh")
        return doc["energy_kwh"]
    except errors.PyMongoError as e:
        print(f"❗ Error upserting energy data for {date.isoformat()}: {e}")
        traceback.print_exc()
        return None


def validate_and_upsert_power_data(power_data, date, energy=None):
    """
    Given payload and dates, does the following:
    1. Verifies whether data is valid.
    2. Calculates daily energy from power data.
    3. Inserts the data into MongoDB, converting timestamps to UTC.

    Args:
        power_data (list): The power data payload from the API.
        date (dt.date): The date for which the data is being validated.
        energy (float, optional): Energy from API to assert against.
    """
    if not power_data:
        print(f"❗ No power data found for {date.isoformat()}. Skipping.")
        return

    if all(r.get("power") in (None, 0) for r in power_data):
        print(
            f"❗ All power records are zero or null for {date.isoformat()}. Skipping."
        )
        return

    docs = []
    total_energy_wh = 0.0
    interval_hours = 5 / 60  # 5 minutes in hours

    for record in power_data:
        if record.get("time"):
            try:
                # Create a naive datetime object from the API string
                naive_timestamp = dt.datetime.fromisoformat(record["time"])

                # **TIMEZONE CHANGE**: Make the timestamp timezone-aware by assigning IST.
                # PyMongo will automatically convert this to UTC upon insertion.
                aware_timestamp = naive_timestamp.replace(tzinfo=IST)

                power = float(record.get("power", 0.0) or 0.0)

                docs.append(
                    {
                        "plant_id": PLANT_ID,
                        "timestamp": aware_timestamp,
                        "power_w": power,
                    }
                )
                # Calculate daily energy in Wh
                total_energy_wh += power * interval_hours
            except (TypeError, ValueError) as e:
                print(
                    f"⚠️  Skipping invalid power record: {record} due to error: {e}"
                )
                continue

    if not docs:
        print(
            f"❗ No valid power records found for {date.isoformat()}. Skipping."
        )
        return

    total_energy_kwh = total_energy_wh / 1000.0
    print(
        f"📊 Total energy calculated from power data for {date.isoformat()}: {total_energy_kwh:.2f} kWh"
    )

    if energy is not None:
        energy_diff = abs(total_energy_kwh - energy)
        if energy_diff > 1.0:
            print(
                f"❗ Energy mismatch for {date.isoformat()}: "
                f"Calculated {total_energy_kwh:.2f} kWh, "
                f"API {energy:.2f} kWh."
            )

    try:
        pwr.insert_many(docs, ordered=False)
        print(f"✅ Inserted {len(docs)} power records for {date.isoformat()}")
    except errors.BulkWriteError as bwe:
        # Filter out duplicate key errors, which are expected and can be ignored
        # Duplicate key error code is 11000
        write_errors = [
            err
            for err in bwe.details.get("writeErrors", [])
            if err.get("code") != 11000
        ]
        if write_errors:
            print(
                f"❗ Error inserting power data for {date.isoformat()}: {write_errors}"
            )
            traceback.print_exc()
        else:
            # All errors were duplicate key errors, which we can treat as a success for upserting behavior
            upserted_count = bwe.details.get("nInserted", 0)
            print(
                f"✅ Upserted {upserted_count} new power records for {date.isoformat()} (skipped duplicates)."
            )


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(
        description="Pull daily power and energy data for a plant from Growatt API."
    )
    parser.add_argument(
        "--date",
        type=str,
        help="The date to pull data for (YYYY-MM-DD). Defaults to today.",
        default=dt.date.today().isoformat(),
    )
    args = parser.parse_args()

    try:
        target_date = dt.date.fromisoformat(args.date)
    except ValueError:
        print(f"❗ Invalid date format: {args.date}. Please use YYYY-MM-DD.")
        sys.exit(1)

    print(f"📅 Starting data pull for {target_date.isoformat()}...")
    pull_daily_data(target_date)
