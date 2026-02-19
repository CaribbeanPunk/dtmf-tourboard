from __future__ import annotations
import textwrap
import streamlit.components.v1 as components


import pandas as pd
import plotly.express as px
import streamlit as st
from pathlib import Path

from tourboard.db import (
    get_conn,
    init_db,
    upsert_events,
    insert_snapshot,
    read_latest_events,
    read_snapshots,
)
from tourboard.scraping import scrape_all, SOURCE_URL
from tourboard.transforms import country_rollup, format_money, format_int, format_price
from tourboard.geocode import geocode_city_country


st.set_page_config(page_title="DTMF Tourboard", layout="wide")

css_path = Path("assets/style.css")
if css_path.exists():
    st.markdown(f"<style>{css_path.read_text()}</style>", unsafe_allow_html=True)


#font titulo
st.markdown(
    """
    <style>
    /* Main hero title */
    .tour-hero {
        text-align: center;
        margin-top: 10px;
        margin-bottom: 30px;
    }

    .tour-title {
        font-family: 'Arial Black', Impact, sans-serif;
        font-size: clamp(36px, 6vw, 64px);
        font-weight: 900;
        letter-spacing: 1px;
        color: #3488C0; /* deep tour blue */
        text-shadow: 3px 3px 0px #ffe84d;
        line-height: 1.05;
    }

    .tour-subtitle {
        font-family: 'Brush Script MT', 'Segoe Script', 'Apple Chancery', cursive;
        font-size: clamp(50px, 3.5vw, 70px);
        font-weight: 700;
        color: #EE3640; /* tour red */
        margin-top: -6px;
        transform: rotate(-3deg)
        text-shadow: 1.5px 1.5px 0px #ffd966;
    
    }
    .poster-header {
    
        display: grid;
        grid-template-columns: 1fr auto;
        align-items: end;
        gap: 16px;
        margin: 10px 0 18px 0;
    }

    .poster-title-wrap {
       text-align: center;
    }

    .poster-title {
    
       font-family: 'Arial Black', Impact, sans-serif;
       font-size: clamp(36px, 6vw, 68px);
       font-weight: 900;
       letter-spacing: 1px;
       color: #3488C0;                 /* tour blue */
       text-shadow: 3px 3px 0px #ffe84d; /* poster yellow pop */
       line-height: 1.02;
    }

    .poster-script {
       font-family: 'Brush Script MT', 'Segoe Script', 'Apple Chancery', cursive;
       font-size: clamp(28px, 4.8vw, 46px);
       font-weight: 700;
       color: #e6392f;                 /* tour red */
       transform: rotate(-6deg);
       display: inline-block;
       margin-top: -10px;
       text-shadow: 1.5px 1.5px 0px #ffd966;
    }

/* character image */
    .poster-frog img {
       width: clamp(120px, 16vw, 160px);
       height: auto;
       filter: drop-shadow(0 10px 12px rgba(0,0,0,0.22));
    }

/* Mobile: stack image under title */
@media (max-width: 640px) {

  .poster-header {
    grid-template-columns: 1fr;
    justify-items: center;
    align-items: center;
  }
  .poster-frog {
    order: 2;
    margin-top: 6px;
  }
  .poster-title-wrap {
    order: 1;
  }
    

    /* Optional: soften Streamlit default padding */
    .block-container {
        padding-top: 1.5rem;
    

    </style>
    """,
    unsafe_allow_html=True,
)


from pathlib import Path
import base64

def img_to_base64(path: str) -> str:
    data = Path(path).read_bytes()
    return base64.b64encode(data).decode("utf-8")

frog_b64 = img_to_base64("assets/frog.png")

st.markdown(
    f"""
    <div class="poster-header">
      <div class="poster-title-wrap">
        <div class="poster-title">DeBÍ TiRAR MáS FOToS</div>
        <div class="poster-script">Tourboard</div>
      </div>
      <div class="poster-frog">
        <img src="data:image/png;base64,{frog_b64}" alt="tour character"/>
      </div>
    </div>
    """,
    unsafe_allow_html=True,
)

conn = get_conn()
init_db(conn)
from tourboard.db import ensure_snapshots_schema
ensure_snapshots_schema(conn)




import pandas as pd
from pathlib import Path

EVENTS_CSV = Path("data/events_latest.csv")
SNAPS_CSV = Path("data/snapshots.csv")

if not EVENTS_CSV.exists():
    st.error("Data file not found yet. The admin needs to run the updater.")
    st.stop()

events = pd.read_csv(EVENTS_CSV)
snaps = pd.read_csv(SNAPS_CSV) if SNAPS_CSV.exists() else pd.DataFrame()


for col in ["gross_usd", "tickets", "shows", "capacity_pct"]:
    if col in events.columns:
        events[col] = pd.to_numeric(events[col], errors="coerce")



# Compute headline metrics from the latest events scrape (more reliable than header parsing)
events_num = events.copy()

# force numeric
events_num["gross_usd"] = pd.to_numeric(events_num["gross_usd"], errors="coerce")
events_num["tickets"] = pd.to_numeric(events_num["tickets"], errors="coerce")
events_num["shows"] = pd.to_numeric(events_num["shows"], errors="coerce")

# totals from event rows (ignores TBA)
reported_revenue = float(events_num["gross_usd"].dropna().sum()) if events_num["gross_usd"].notna().any() else None
reported_tickets = int(events_num["tickets"].dropna().sum()) if events_num["tickets"].notna().any() else None

avg_price = (reported_revenue / reported_tickets) if (reported_revenue is not None and reported_tickets not in (None, 0)) else None

# shows (optional but good)
total_shows = int(events_num["shows"].fillna(0).sum()) if "shows" in events_num.columns else 0
reported_shows = int(events_num.loc[events_num["gross_usd"].notna(), "shows"].fillna(0).sum()) if "shows" in events_num.columns else 0

total_reports_text = f"{reported_shows} / {total_shows} shows reported"

#total countries
total_countries = events.loc[events["gross_usd"].notna(), "country"].dropna().nunique()


# Last update timestamp (from latest scrape)
if "scraped_at" in events.columns and events["scraped_at"].notna().any():
    last_updated = events["scraped_at"].max()
else:
    last_updated = None


#next stop
from datetime import datetime
import pycountry
import re

def country_to_flag(country_name: str) -> str:
    """
    Convert country name -> flag emoji.
    """
    try:
        country = pycountry.countries.search_fuzzy(country_name)[0]
        return "".join(chr(0x1F1E6 + ord(c) - ord("A")) for c in country.alpha_2)
    except Exception:
        return "🏳️"

from datetime import datetime, date
import re

def _month_to_num(m: str) -> int:
    m = m.strip().replace(".", "")
    # accept full or abbreviated month names
    try:
        return datetime.strptime(m[:3], "%b").month
    except Exception:
        return datetime.strptime(m, "%B").month

def parse_start_date(date_range: str):
    """
    Returns a datetime.date for the FIRST day of the run.
    Handles:
      - "November 21-22, 2025"
      - "December 10-21, 2025"
      - "February 28-Mar. 1, 2026"
      - "July 1, 2026"
    """
    if not date_range:
        return None

    s = date_range.strip()

    # Cross-month: "Feb 28-Mar. 1, 2026"
    m = re.match(r"^([A-Za-z]{3,}\.?)\s+(\d{1,2})-([A-Za-z]{3,}\.?)\s+(\d{1,2}),\s*(\d{4})$", s)
    if m:
        m1, d1, m2, d2, y = m.groups()
        return date(int(y), _month_to_num(m1), int(d1))

    # Same-month range or single date: "November 21-22, 2025" or "July 1, 2026"
    m = re.match(r"^([A-Za-z]{3,}\.?)\s+(\d{1,2})(?:-\d{1,2})?,\s*(\d{4})$", s)
    if m:
        mon, d1, y = m.groups()
        return date(int(y), _month_to_num(mon), int(d1))

    return None

#date range parser para banner

from datetime import date, datetime, timedelta
import re

def _month_to_num(m: str) -> int:
    m = m.strip().replace(".", "")
    return datetime.strptime(m[:3], "%b").month

def parse_date_range(date_range: str):
    """
    Returns (start_date, end_date) as datetime.date
    Handles:
      - "November 21-22, 2025"
      - "December 10-21, 2025"
      - "February 28-Mar. 1, 2026"
      - "July 1, 2026"
    """
    if not date_range:
        return (None, None)

    s = date_range.strip()

    # Cross-month: "Feb 28-Mar. 1, 2026"
    m = re.match(r"^([A-Za-z]{3,}\.?)\s+(\d{1,2})-([A-Za-z]{3,}\.?)\s+(\d{1,2}),\s*(\d{4})$", s)
    if m:
        m1, d1, m2, d2, y = m.groups()
        y = int(y)
        start = date(y, _month_to_num(m1), int(d1))
        end = date(y, _month_to_num(m2), int(d2))
        return (start, end)

    # Same-month range: "November 21-22, 2025" or single "July 1, 2026"
    m = re.match(r"^([A-Za-z]{3,}\.?)\s+(\d{1,2})(?:-(\d{1,2}))?,\s*(\d{4})$", s)
    if m:
        mon, d1, d2, y = m.groups()
        y = int(y)
        start = date(y, _month_to_num(mon), int(d1))
        end = date(y, _month_to_num(mon), int(d2)) if d2 else start
        return (start, end)

    return (None, None)


# --- Next Stop (upcoming, not yet reported) ---
from datetime import date

today = date.today()

status_df = events.copy()
status_df[["start_dt", "end_dt"]] = status_df["date_range"].apply(
    lambda s: pd.Series(parse_date_range(s))
)

# Current stop = any run where today is within [start_dt, end_dt]
current = status_df[
    status_df["start_dt"].notna() &
    status_df["end_dt"].notna() &
    (status_df["start_dt"] <= today) &
    (today <= status_df["end_dt"])
].sort_values("start_dt").head(1)

current_data = current.iloc[0].to_dict() if not current.empty else None

# Next stop = earliest run whose start_dt is in the future
next_run = status_df[
    status_df["start_dt"].notna() &
    (status_df["start_dt"] > today)
].sort_values("start_dt").head(1)

next_data = next_run.iloc[0].to_dict() if not next_run.empty else None

# Choose banner mode/data
if current_data:
    banner_mode = "current"
    banner_data = current_data
else:
    banner_mode = "next"
    banner_data = next_data


from datetime import date

# --- Latest report available (most recent stop with gross reported) ---
reports_df = events.copy()
reports_df["gross_usd"] = pd.to_numeric(reports_df["gross_usd"], errors="coerce")
reports_df["tickets"] = pd.to_numeric(reports_df["tickets"], errors="coerce")

reports_df[["start_dt", "end_dt"]] = reports_df["date_range"].apply(
    lambda s: pd.Series(parse_date_range(s))
)

reported = reports_df[
    reports_df["gross_usd"].notna()
    & reports_df["tickets"].notna()
    & reports_df["start_dt"].notna()
].copy()

# pick the most recent *reported* stop by end date (fallback start date)
if not reported.empty:
    reported["sort_dt"] = reported["end_dt"].fillna(reported["start_dt"])
    latest_report = reported.sort_values("sort_dt", ascending=False).head(1)
    latest_report_data = latest_report.iloc[0].to_dict()
else:
    latest_report_data = None




c1, c2, c3, c4, c5 = st.columns(5)
with c1:
    st.markdown(
        f'<div class="tb-card"><div class="tb-badge">TOTAL REVENUE</div><div class="tb-metric">{format_money(reported_revenue)}</div><div class="tb-muted">reported</div></div>',
        unsafe_allow_html=True,
    )
with c2:
    st.markdown(
        f'<div class="tb-card"><div class="tb-badge">TOTAL TICKETS SOLD</div><div class="tb-metric">{format_int(reported_tickets)}</div><div class="tb-muted">reported</div></div>',
        unsafe_allow_html=True,
    )

with c3:
    st.markdown(
        f'<div class="tb-card"><div class="tb-badge">AVG TICKET PRICE</div><div class="tb-metric">{format_price(avg_price)}</div><div class="tb-muted">derived</div></div>',
        unsafe_allow_html=True,
    )
with c4:
    st.markdown(
        f'<div class="tb-card"><div class="tb-badge">REPORTED SHOWS</div><div class="tb-metric">{reported_shows}</div><div class="tb-muted">{total_reports_text}</div></div>',
        unsafe_allow_html=True,
    )

with c5:
    st.markdown(
        f'<div class="tb-card"><div class="tb-badge">TOTAL COUNTRIES VISITED</div><div class="tb-metric">{total_countries}</div><div class="tb-muted">tour stops</div></div>',
        unsafe_allow_html=True,
    )
    

st.markdown("### ⏱️ Tour Status")

if banner_data:
    flag = country_to_flag(banner_data["country"])

    if banner_mode == "current":
        label = "🎤 CURRENT STOP"
        subtitle = "Happening now"
    else:
        label = "✈️ NEXT STOP"
        subtitle = "Upcoming"

    report_status = (
        "✅ Reported"
        if pd.notna(banner_data.get("gross_usd"))
        else "⏳ Pending report"
    )

    html = f"""
    <div style="
        background: linear-gradient(135deg, #fff7cc, #fff1a8);
        border-radius: 16px;
        padding: 20px;
        margin-bottom: 24px;
        box-shadow: 0 8px 20px rgba(0,0,0,0.08);
        border: 2px solid rgba(0,0,0,0.15);
        font-family: system-ui, -apple-system, Segoe UI, Roboto, Arial, sans-serif;
    ">
        <div style="display:flex; justify-content:space-between; align-items:flex-start;">
            <div>
                <div style="font-size:14px; letter-spacing:1px; opacity:0.75; font-weight:800;">
                    {label}
                </div>
                <div style="font-size:13px; opacity:0.65; margin-top:2px;">
                    {subtitle}
                </div>
            </div>
            <div style="font-size:16px; opacity:0.85; font-weight:700;">
                {report_status}
            </div>
        </div>

        <div style="font-size:28px; font-weight:900; margin-top:12px;">
            {flag} {banner_data["city"]}, {banner_data["country"]}
        </div>

        <div style="margin-top:10px; font-size:18px;">
            🗓️ {banner_data["date_range"]}
        </div>

        <div style="margin-top:6px; font-size:16px; opacity:0.85;">
            📍 {banner_data["venue"]}
        </div> 

    </div>
    """

    components.html(html, height=260, scrolling=False)


else:
    st.info("No upcoming stops found in the schedule.")



if latest_report_data:
    flag2 = country_to_flag(latest_report_data["country"])

    # nice rounded labels
    gross_m = int(round(latest_report_data["gross_usd"] / 1_000_000))
    gross_label = f"${gross_m}M"
    tickets_k = int(round(latest_report_data["tickets"] / 1_000))
    tickets_label = f"{tickets_k}K"

    html_report = f"""
    <div style="
        background: linear-gradient(135deg, #f2f2f2, #ffffff);
        border-radius: 16px;
        padding: 18px 20px;
        margin-bottom: 24px;
        box-shadow: 0 8px 20px rgba(0,0,0,0.06);
        border: 2px solid rgba(0,0,0,0.10);
        font-family: system-ui, -apple-system, Segoe UI, Roboto, Arial, sans-serif;
    ">
        <div style="display:flex; justify-content:space-between; align-items:flex-start;">
            <div>
                <div style="font-size:14px; letter-spacing:1px; opacity:0.75; font-weight:800;">
                    🧾 LATEST REPORT AVAILABLE
                </div>
                <div style="font-size:13px; opacity:0.65; margin-top:2px;">
                    Most recently published box office & tickets
                </div>
            </div>
            <div style="font-size:13px; opacity:0.65; font-weight:700;">
                Source: Touring Data
            </div>
        </div>

        <div style="font-size:26px; font-weight:900; margin-top:12px;">
            {flag2} {latest_report_data["city"]}, {latest_report_data["country"]}
        </div>

        <div style="margin-top:8px; font-size:16px; opacity:0.85;">
            🗓️ {latest_report_data["date_range"]} &nbsp;&nbsp; • &nbsp;&nbsp; 📍 {latest_report_data["venue"]}
        </div>

        <div style="display:flex; gap:16px; margin-top:14px; flex-wrap:wrap;">
            <div style="background: rgba(255,233,77,0.35); padding:10px 12px; border-radius: 12px; border: 1px solid rgba(0,0,0,0.08);">
                <div style="font-size:12px; opacity:0.7; font-weight:800;">REPORTED GROSS</div>
                <div style="font-size:20px; font-weight:900;">{gross_label}</div>
            </div>
            <div style="background: rgba(255,233,77,0.35); padding:10px 12px; border-radius: 12px; border: 1px solid rgba(0,0,0,0.08);">
                <div style="font-size:12px; opacity:0.7; font-weight:800;">REPORTED TICKETS</div>
                <div style="font-size:20px; font-weight:900;">{tickets_label}</div>
            </div>
            <div style="background: rgba(255,233,77,0.35); padding:10px 12px; border-radius: 12px; border: 1px solid rgba(0,0,0,0.08);">
                <div style="font-size:12px; opacity:0.7; font-weight:800;">AVG PRICE</div>
                <div style="font-size:20px; font-weight:900;">${latest_report_data["gross_usd"]/latest_report_data["tickets"]:.2f}</div>
           
            </div>
        </div>
    </div>
    """

    components.html(html_report, height=320, scrolling=False)

else:
    st.info("No reported box office data found yet.")


st.markdown("---")

#left, right = st.columns([1, 2], gap="large")


    

st.markdown("#### 🔎 Filter")

region_opts = ["All"] + sorted([x for x in events["region"].dropna().unique()])
region_choice = st.selectbox("Region", region_opts, index=0)


country_opts = ["All"] + sorted([x for x in events["country"].dropna().unique()])
country_choice = st.selectbox("Country", country_opts, index=0)

filtered = events.copy()
if region_choice != "All":
    filtered = filtered[filtered["region"] == region_choice]
    
    
        
if country_choice != "All":
    filtered = filtered[filtered["country"] == country_choice]

#st.markdown("#### 🗓️ Complete Tour Dates")


cols = ["region", "date_range", "venue", "city", "country", "gross_usd", "tickets", "shows"]
view = filtered[cols].copy()
view["gross_usd"] = view["gross_usd"].map(format_money)
view["tickets"] = view["tickets"].map(format_int)
#st.dataframe(view, use_container_width=True, hide_index=True)
with st.expander("🗓️ Complete Tour Dates", expanded=False):
    st.dataframe(view, use_container_width=True, hide_index=True)


# --- Country rollup used by charts (roll) ---
roll = (
    events.dropna(subset=["country"])
    .groupby(["country"], as_index=False)
    .agg(
        gross_usd=("gross_usd", "sum"),
        tickets=("tickets", "sum"),
        shows=("shows", "sum"),
    )
)

# Avoid zeros becoming weird in charts
roll.loc[roll["gross_usd"] == 0, "gross_usd"] = pd.NA
roll.loc[roll["tickets"] == 0, "tickets"] = pd.NA


    
st.markdown("### 📊 Charts")

tix_df = roll.dropna(subset=["gross_usd"]).copy()
tix_df = tix_df[tix_df["gross_usd"] > 0].sort_values("gross_usd", ascending=True)

tix_df["gross_M"] = (tix_df["gross_usd"] / 1_000_000).round(0)
tix_df["gross_label"] = "$"+ tix_df["gross_M"].astype(int).astype(str) + "M"


fig_tix = px.bar(
       
    tix_df,
    x="gross_usd",
    y="country",
    orientation="h",
    title="Reported Revenue Generated by Country",
)
fig_tix.update_layout(margin=dict(l=0, r=90, t=60, b=0))

max_x = tix_df["gross_usd"].max()
fig_tix.update_xaxes(range=[0, max_x * 1.15])


fig_tix.update_traces(
    text=tix_df["gross_label"],
    textposition="outside",
    hovertemplate="$%{x:,.0f}<extra></extra>",
    cliponaxis=False,
)


st.plotly_chart(fig_tix, use_container_width=True, config={"responsive": True})




# --- Revenue per show by country (efficiency) ---

rps_df = roll.dropna(subset=["gross_usd"]).copy()
rps_df = rps_df[rps_df["gross_usd"] > 0]

# Group to compute:
# - reported shows count
# - total reported gross
# - revenue per show
rps_agg = (
    rps_df.groupby("country", as_index=False)
    .agg(
        reported_shows=("gross_usd", "count"),
        reported_gross_usd=("gross_usd", "sum"),
    )
)

rps_agg["revenue_per_show_usd"] = rps_agg["reported_gross_usd"] / rps_agg["reported_shows"]



# Sort so the biggest is on top (like your other chart)
rps_agg = rps_agg.sort_values("revenue_per_show_usd", ascending=True)

# Labels like "$12M" but for per-show (still in millions)
rps_agg["rps_M"] = (rps_agg["revenue_per_show_usd"] / 1_000_000).round(1)
rps_agg["rps_label"] = "$" + rps_agg["rps_M"].astype(str) + "M"

fig_rps = px.bar(
    rps_agg,
    x="revenue_per_show_usd",
    y="country",
    orientation="h",
    title=f"Revenue per show by country",
)

fig_rps.update_layout(margin=dict(l=0, r=90, t=60, b=0))

max_x = rps_agg["revenue_per_show_usd"].max()
fig_rps.update_xaxes(range=[0, max_x * 1.15])

fig_rps.update_traces(
    text=rps_agg["rps_label"],
    textposition="outside",
    hovertemplate=(
        "Revenue/show: $%{x:,.0f}"
        "<br>Reported shows: %{customdata[0]}"
        "<br>Total reported gross: $%{customdata[1]:,.0f}"
        "<extra></extra>"
    ),
    customdata=rps_agg[["reported_shows", "reported_gross_usd"]].to_numpy(),
    cliponaxis=False,
)

st.plotly_chart(fig_rps, use_container_width=True, config={"responsive": True})




#Tickets sold by country


tix_df = roll.dropna(subset=["tickets"]).copy()
tix_df = tix_df[tix_df["tickets"] > 0].sort_values("tickets", ascending=True)

tix_df["tickets_K"] = (tix_df["tickets"] / 1_000).round(0)
tix_df["tickets_label"] = tix_df["tickets_K"].astype(int).astype(str) + "K"


fig_tix = px.bar(
    tix_df,
    x="tickets",
    y="country",
    orientation="h",
    title="Reported Tickets Sold by Country",
)
    
    
fig_tix.update_layout(margin=dict(l=0, r=90, t=60, b=0))

max_x = tix_df["tickets"].max()
fig_tix.update_xaxes(range=[0, max_x * 1.15])


fig_tix.update_traces(
    text=tix_df["tickets_label"],
    textposition="outside",
    hovertemplate="$%{x:,.0f}<extra></extra>",
    cliponaxis=False,
)
    



st.plotly_chart(fig_tix, use_container_width=True, config={"responsive": True})


        # ===============================
    # Avg Ticket Price by City
    # ===============================


city_df = events.copy()
city_df["gross_usd"] = pd.to_numeric(city_df["gross_usd"], errors="coerce")
city_df["tickets"] = pd.to_numeric(city_df["tickets"], errors="coerce")

    # Keep only rows with reported data
city_df = city_df.dropna(subset=["gross_usd", "tickets", "country"])
city_df = city_df[city_df["tickets"] > 0]

    # Aggregate by city + country
city_roll = (
    
    city_df.groupby(["country"], as_index=False)
    .agg(      
        gross_usd=("gross_usd", "sum"),
        tickets=("tickets", "sum"),      
        )
    )



city_roll["avg_price_usd"] = city_roll["gross_usd"] / city_roll["tickets"]
city_roll["country_label"] = city_roll["country"]



    
fig_city_price = px.bar(
    city_roll,
    x="avg_price_usd",
    y="country",
    orientation="h",
    title="Avg. Ticket Price By Country",
        
)


fig_city_price.update_layout(margin=dict(l=0, r=90, t=60, b=0))

max_x = tix_df["tickets"].max()
fig_tix.update_xaxes(range=[0, max_x * 1.15]

)


fig_city_price.update_traces(
    texttemplate="$%{x:,.0f}",
    textposition="outside",
    hovertemplate="$%{x:,.2f}<extra></extra>",
    cliponaxis=False,
)


st.plotly_chart(fig_city_price, use_container_width=True,config={"responsive": True})



# =========================
# Prepare map points (ensure lat/lon exist)
# =========================

points = events.copy()

# Status: reported vs pending
from datetime import date

today = date.today()

# Parse start / end dates from date_range
points[["start_dt", "end_dt"]] = points["date_range"].apply(
    lambda s: pd.Series(parse_date_range(s))
)

def tour_status(row):
    if row["start_dt"] and row["end_dt"]:
        if row["start_dt"] <= today <= row["end_dt"]:
            return "Current stop"
        elif row["end_dt"] < today:
            return "Happened"
    return "Upcoming"

points["status"] = points.apply(tour_status, axis=1)



# Create lat/lon columns
points["lat"] = pd.NA
points["lon"] = pd.NA

# Geocode unique city-country pairs (cached in SQLite)
unique_places = (
    points[["city", "country"]]
    .dropna()
    .drop_duplicates()
    .values
)

for city, country in unique_places:
    res = geocode_city_country(conn, city, country)
    if res:
        lat, lon = res
        mask = (points["city"] == city) & (points["country"] == country)
        points.loc[mask, "lat"] = lat
        points.loc[mask, "lon"] = lon

# Keep only rows with coordinates
points = points.dropna(subset=["lat", "lon"]).copy()

# Hover display helpers
points["gross_display"] = points["gross_usd"].apply(format_money)
points["tickets_display"] = points["tickets"].apply(format_int)

st.markdown("## 🌍🎤 Tour Map")

fig_map = px.scatter_mapbox(
    points,
    lat="lat",
    lon="lon",
    color="status",
    hover_name="city",
    hover_data={
        "country": True,
        "venue": True,
        "date_range": True,
        "shows": True,
        "gross_display": True,
        "tickets_display": True,
        "lat": False,
        "lon": False,
        "status": False,
    },
    zoom=2,
    height=520,
)

# OpenStreetMap tiles (no token needed)
fig_map.update_layout(
    mapbox_style="open-street-map",
    margin={"r": 0, "t": 0, "l": 0, "b": 0},
    legend_title_text="Tour Status",
)

# Bigger dots + force colors
fig_map.for_each_trace(
    lambda t: t.update(marker=dict(
        size=15,
        opacity=0.85,
        color=(
            "gold" if "Current stop" in t.name
            else "green" if "Happened" in t.name
            else "red"
        ),
    ))
)



# Auto-center/zoom to your points (removes Antarctica problem entirely)
fig_map.update_layout(
    mapbox_bounds={
        "west": float(points["lon"].min()) - 5,
        "east": float(points["lon"].max()) + 5,
        "south": float(points["lat"].min()) - 5,
        "north": float(points["lat"].max()) + 5,
    }
)



st.plotly_chart(fig_map, use_container_width=True,config={"responsive": True})

                  
           
st.markdown("---")
st.caption(f"Made By: Luis Macfie: www.linkedin.com/in/luis-macfie/")
st.caption(f"Source: Touring Data tour page • {SOURCE_URL}")

