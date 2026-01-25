from __future__ import annotations

import pandas as pd
import numpy as np


def country_rollup(events: pd.DataFrame) -> pd.DataFrame:
    df = events.copy()

    df["gross_usd"] = pd.to_numeric(df["gross_usd"], errors="coerce")
    df["tickets"] = pd.to_numeric(df["tickets"], errors="coerce")
    df["shows"] = pd.to_numeric(df["shows"], errors="coerce")

    grp = df.groupby("country", dropna=False, as_index=False).agg(
        gross_usd=("gross_usd", "sum"),
        tickets=("tickets", "sum"),
        shows=("shows", "sum"),
        runs=("venue", "count"),
    )

    grp["avg_price_usd"] = np.where(
        (grp["tickets"] > 0) & (grp["gross_usd"].notna()),
        grp["gross_usd"] / grp["tickets"],
        np.nan,
    )

    return grp.sort_values("gross_usd", ascending=False, na_position="last")


def format_money(x: float) -> str:
    if pd.isna(x):
        return "—"
    return "${:,.0f}".format(float(x))


def format_int(x: float) -> str:
    if pd.isna(x):
        return "—"
    return "{:,.0f}".format(float(x))


def format_price(x: float) -> str:
    if pd.isna(x):
        return "—"
    return "${:,.2f}".format(float(x))
