from __future__ import annotations

import sqlite3
from pathlib import Path
from typing import Optional
import pandas as pd

DB_PATH = Path("data") / "tourboard.sqlite"


def get_conn(db_path: Optional[Path] = None) -> sqlite3.Connection:
    path = db_path or DB_PATH
    path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(path)
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA foreign_keys=ON;")
    return conn


def init_db(conn):
    conn.executescript(
        """
        CREATE TABLE IF NOT EXISTS snapshots (
            scraped_at TEXT,
            source_url TEXT,
            reported_revenue_usd REAL,
            reported_tickets INTEGER,
            avg_price_usd REAL,
            total_reports_text TEXT
        );

        CREATE TABLE IF NOT EXISTS events (
            region TEXT,
            date_range TEXT,
            start_date TEXT,
            end_date TEXT,
            artist TEXT,
            venue TEXT,
            city TEXT,
            country TEXT,
            gross_usd REAL,
            tickets INTEGER,
            capacity_pct REAL,
            shows INTEGER,
            source_url TEXT,
            scraped_at TEXT
        );

        CREATE TABLE IF NOT EXISTS geocache (
            key TEXT PRIMARY KEY,
            city TEXT,
            country TEXT,
            lat REAL,
            lon REAL,
            provider TEXT,
            updated_at TEXT
        );
        """

def ensure_snapshots_schema(conn: sqlite3.Connection) -> None:
    """
    Add missing columns to snapshots table to handle schema changes over time.
    Safe to run on every startup.
    """
    cur = conn.execute("PRAGMA table_info(snapshots)")
    existing = {row[1] for row in cur.fetchall()}  # row[1] = column name

    desired = {
        "scraped_at": "TEXT",
        "reported_revenue_usd": "REAL",
        "reported_tickets": "INTEGER",
        "avg_revenue_usd": "REAL",
        "avg_tickets": "INTEGER",
        "avg_price_usd": "REAL",
        "total_reports_text": "TEXT",
        "source_url": "TEXT",
    }

    # Add any missing columns
    for col, col_type in desired.items():
        if col not in existing:
            conn.execute(f"ALTER TABLE snapshots ADD COLUMN {col} {col_type};")

    conn.commit()



def upsert_events(conn: sqlite3.Connection, df: pd.DataFrame) -> None:
    df.to_sql("events", conn, if_exists="append", index=False)


def insert_snapshot(conn: sqlite3.Connection, snap: dict) -> None:
    pd.DataFrame([snap]).to_sql("snapshots", conn, if_exists="append", index=False)


def read_latest_events(conn: sqlite3.Connection) -> pd.DataFrame:
    q = """
    SELECT * FROM events
    WHERE scraped_at = (SELECT MAX(scraped_at) FROM events)
    """
    return pd.read_sql_query(q, conn)


def read_snapshots(conn: sqlite3.Connection) -> pd.DataFrame:
    return pd.read_sql_query("SELECT * FROM snapshots ORDER BY scraped_at ASC", conn)

#Para Mapa

from datetime import datetime, timezone

def _utc_now_iso():
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()

def geocache_get(conn: sqlite3.Connection, key: str):
    cur = conn.execute("SELECT lat, lon FROM geocache WHERE key = ?", (key,))
    row = cur.fetchone()
    return row if row else None

def geocache_set(conn: sqlite3.Connection, key: str, city: str, country: str, lat: float, lon: float, provider: str = "nominatim"):
    conn.execute(
        "INSERT OR REPLACE INTO geocache (key, city, country, lat, lon, provider, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?)",
        (key, city, country, lat, lon, provider, _utc_now_iso()),
    )
    conn.commit()

