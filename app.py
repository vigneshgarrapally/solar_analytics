#!/usr/bin/env python3
"""
Solar Performance Dashboard (Streamlit)
--------------------------------------
Visualize Growatt solar data stored in MongoDB.
  • power_readings (5-minute raw power)
  • daily_energy   (daily kWh summaries)

Features
********
- Period selector: Day / Week / Month / Year.
- Contextual pickers:
    Day   → st.date_input
    Week  → st.date_input with a range (tuple)
    Month → Select month & year (helper to compute range)
    Year  → Select year
- Auto-query MongoDB, converting IST ⇄ UTC.
- Plotly interactive charts (area, bar, line) + KPI cards.
- Simple theming tweaks with custom CSS.

Run      : streamlit run solar_streamlit_app.py
Env vars : MONGODB_URI (connection string)
"""
import os
import datetime as dt
from dateutil.relativedelta import relativedelta
import pytz
import pandas as pd
from dotenv import load_dotenv
from pymongo import MongoClient
import plotly.express as px
import streamlit as st

# ---------------- Config ----------------
IST = pytz.timezone("Asia/Kolkata")
UTC = pytz.UTC

load_dotenv()
MONGO_URI = os.getenv("MONGODB_URI") or st.secrets.get("MONGODB_URI")

if not MONGO_URI:
    st.stop()


# ---------------- Helpers ----------------
@st.cache_resource(show_spinner=False)
def get_db_client(uri: str):
    return MongoClient(uri)


client = get_db_client(MONGO_URI)
db = client["solar_data_v2"]
PWR_COL = db["power_readings"]
ENERGY_COL = db["daily_energy"]


def ist_day_window(date_ist: dt.date):
    """Return (utc_start, utc_end) for the IST day."""
    ist_start = IST.localize(dt.datetime.combine(date_ist, dt.time.min))
    ist_end = IST.localize(dt.datetime.combine(date_ist, dt.time.max))
    return ist_start.astimezone(UTC), ist_end.astimezone(UTC)


@st.cache_data(show_spinner=False)
def fetch_power(start_utc: dt.datetime, end_utc: dt.datetime) -> pd.DataFrame:
    cur = PWR_COL.find(
        {"timestamp": {"$gte": start_utc, "$lte": end_utc}},
        {"_id": 0, "timestamp": 1, "power_w": 1},
    )
    df = pd.DataFrame(cur)
    if df.empty:
        return df
    df = df.sort_values("timestamp")
    df["timestamp"] = (
        pd.to_datetime(df["timestamp"])
        .dt.tz_localize("UTC")
        .dt.tz_convert(IST)
    )
    return df


@st.cache_data(show_spinner=False)
def fetch_energy(
    start_date_ist: dt.date, end_date_ist: dt.date
) -> pd.DataFrame:
    start_utc, _ = ist_day_window(start_date_ist)
    _, end_utc = ist_day_window(end_date_ist)
    cur = ENERGY_COL.find(
        {"date": {"$gte": start_utc, "$lte": end_utc}},
        {"_id": 0, "date": 1, "energy_kwh": 1},
    ).sort("date", 1)
    df = pd.DataFrame(cur)
    if df.empty:
        return df
    df["date"] = (
        pd.to_datetime(df["date"])
        .dt.tz_localize("UTC")
        .dt.tz_convert(IST)
        .dt.date
    )
    return df


def month_date_range(any_date: dt.date):
    start = any_date.replace(day=1)
    end = (start + relativedelta(months=1)) - dt.timedelta(days=1)
    return start, end


def year_date_range(year: int):
    return dt.date(year, 1, 1), dt.date(year, 12, 31)


# ---------------- UI ----------------
st.set_page_config(
    page_title="Solar Performance Dashboard",
    layout="wide",
    initial_sidebar_state="collapsed",
)

CUSTOM_CSS = """
/* Subtle tweak to Plotly tooltip font & card look */
div[data-testid="metric-container"] > div {background: #f8f9fa; border-radius: 0.5rem; padding: 0.75rem;}
.block-container {padding-top: 1.2rem;}
"""
st.markdown(f"<style>{CUSTOM_CSS}</style>", unsafe_allow_html=True)

st.title("⚡ Solar Performance Dashboard")

period = st.radio(
    "Select period",
    ["Day", "Week", "Month", "Year"],
    horizontal=True,
    index=0,
)

# Containers for controls and charts
ctrl_col, _ = st.columns([2, 1])

with ctrl_col:
    today = dt.date.today()
    if period == "Day":
        sel_day = st.date_input("Pick a date", value=today, key="day")
    elif period == "Week":
        sel_week = st.date_input(
            "Pick a week (range)",
            value=(today - dt.timedelta(days=today.weekday()), today),
            key="week",
        )
    elif period == "Month":
        mcol1, mcol2 = st.columns(2)
        with mcol1:
            sel_month = st.selectbox(
                "Month",
                list(range(1, 13)),
                index=today.month - 1,
                format_func=lambda m: dt.date(2000, m, 1).strftime("%b"),
            )
        with mcol2:
            sel_year_for_month = st.number_input(
                "Year",
                min_value=2010,
                max_value=today.year,
                value=today.year,
                step=1,
            )
    else:  # Year
        sel_year = st.number_input(
            "Year",
            min_value=2010,
            max_value=today.year,
            value=today.year,
            step=1,
        )

# ---------------- Logic & Charts ----------------
fig = None
metrics = {}

if period == "Day":
    if isinstance(sel_day, list):
        sel_day = sel_day[0]
    start_utc, end_utc = ist_day_window(sel_day)
    df_pwr = fetch_power(start_utc, end_utc)
    if df_pwr.empty:
        st.warning("No power data available for this date.")
    else:
        # Energy by rectangle rule (5 min intervals)
        energy_kwh = (df_pwr["power_w"].sum() * 5) / 60 / 1000
        fig = px.area(
            df_pwr,
            x="timestamp",
            y="power_w",
            labels={"timestamp": "Time (IST)", "power_w": "Power (W)"},
            title=f"Power Curve – {sel_day.strftime('%d %b %Y')}",
        )
        fig.update_layout(hovermode="x unified")
        metrics = {
            "Total Energy (kWh)": f"{energy_kwh:.2f}",
            "Peak Power (W)": f"{df_pwr['power_w'].max():.0f}",
            "Average Power (W)": f"{df_pwr['power_w'].mean():.0f}",
        }

elif period == "Week":
    if not isinstance(sel_week, (list, tuple)) or len(sel_week) != 2:
        st.info("Please pick a valid start and end date.")
    else:
        start_date, end_date = sel_week
        df_en = fetch_energy(start_date, end_date)
        if df_en.empty:
            st.warning("No energy data available for this week.")
        else:
            fig = px.bar(
                df_en,
                x="date",
                y="energy_kwh",
                labels={"date": "Date", "energy_kwh": "Energy (kWh)"},
                title=f"Daily Energy - {start_date.strftime('%d %b %Y')} to {end_date.strftime('%d %b %Y')}",
            )
            fig.update_xaxes(dtick="D1", tickformat="%d %b")
            fig.update_layout(hovermode="x unified")
            metrics = {
                "Week Total (kWh)": f"{df_en['energy_kwh'].sum():.2f}",
                "Best Day": df_en.loc[
                    df_en["energy_kwh"].idxmax(), "date"
                ].strftime("%d %b %Y"),
            }

elif period == "Month":
    start_m, end_m = month_date_range(
        dt.date(sel_year_for_month, sel_month, 1)
    )
    df_en = fetch_energy(start_m, end_m)
    if df_en.empty:
        st.warning("No energy data available for this month.")
    else:
        fig = px.line(
            df_en,
            x="date",
            y="energy_kwh",
            labels={"date": "Date", "energy_kwh": "Energy (kWh)"},
            title=start_m.strftime("Daily Energy – %B %Y"),
        )
        # print all energy values in the console for debugging
        print(df_en["energy_kwh"].to_list())
        fig.update_xaxes(dtick="D1", tickformat="%d %b")
        fig.update_layout(hovermode="x unified")
        metrics = {
            "Month Total (kWh)": f"{df_en['energy_kwh'].sum():.2f}",
            "Avg per Day (kWh)": f"{df_en['energy_kwh'].mean():.2f}",
        }

elif period == "Year":
    start_y, end_y = year_date_range(int(sel_year))
    df_en = fetch_energy(start_y, end_y)  # returns IST dates already
    if df_en.empty:
        st.warning("No energy data available for this year.")
    else:
        df_en["month"] = df_en["date"].apply(lambda d: d.replace(day=1))
        df_year = (
            df_en.groupby("month", as_index=False)["energy_kwh"]
            .sum()
            .rename(columns={"energy_kwh": "energy"})
        )

        fig = px.bar(
            df_year,
            x="month",
            y="energy",
            labels={"month": "Month", "energy": "Energy (kWh)"},
            title=f"Monthly Energy – {sel_year}",
        )
        fig.update_xaxes(dtick="M1", tickformat="%b %Y")
        fig.update_layout(hovermode="x unified")

        metrics = {"Year Total (kWh)": f"{df_year['energy'].sum():.2f}"}

# ---------------- Display ----------------
if metrics:
    mcols = st.columns(len(metrics))
    for (label, value), col in zip(metrics.items(), mcols):
        col.metric(label, value)

if fig is not None:
    st.plotly_chart(
        fig, use_container_width=True, config={"displaylogo": False}
    )

st.caption("Data timezone stored in UTC; converted to IST for display.")
