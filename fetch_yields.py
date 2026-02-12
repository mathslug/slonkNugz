#!/usr/bin/env python3
"""Fetch Treasury CMT daily yield curve data and store in the DB."""

import argparse
import csv
import io
from datetime import date, datetime

import requests

import db as db_mod

# Treasury.gov CSV URL template
_URL_TEMPLATE = (
    "https://home.treasury.gov/resource-center/data-chart-center/interest-rates/"
    "daily-treasury-rates.csv/{year}/all?type=daily_treasury_yield_curve"
    "&field_tdr_date_value={year}&page&_format=csv"
)

# CSV column name -> DB column name
_COLUMN_MAP = {
    "1 Mo": "m1", "1.5 Month": "m1h", "2 Mo": "m2", "3 Mo": "m3", "4 Mo": "m4", "6 Mo": "m6",
    "1 Yr": "y1", "2 Yr": "y2", "3 Yr": "y3", "5 Yr": "y5", "7 Yr": "y7",
    "10 Yr": "y10", "20 Yr": "y20", "30 Yr": "y30",
}


def _parse_rate(val: str) -> float | None:
    """Parse a rate string, returning None for empty/N/A values."""
    if not val or val.strip() in ("", "N/A"):
        return None
    try:
        return float(val)
    except ValueError:
        return None


def fetch_csv(year: int) -> list[dict]:
    """Fetch Treasury CMT CSV for a given year and return parsed rows."""
    url = _URL_TEMPLATE.format(year=year)
    resp = requests.get(url, timeout=30)
    resp.raise_for_status()
    reader = csv.DictReader(io.StringIO(resp.text))
    rows = []
    for raw in reader:
        date_str = raw.get("Date", "").strip()
        if not date_str:
            continue
        # Parse date — Treasury uses MM/DD/YYYY
        try:
            dt = datetime.strptime(date_str, "%m/%d/%Y")
        except ValueError:
            continue
        row = {"date": dt.strftime("%Y-%m-%d")}
        for csv_col, db_col in _COLUMN_MAP.items():
            row[db_col] = _parse_rate(raw.get(csv_col, ""))
        rows.append(row)
    return rows


def main():
    parser = argparse.ArgumentParser(description="Fetch Treasury CMT yield curve data")
    parser.add_argument("--db", default="kalshi_arb.db", help="SQLite database path")
    args = parser.parse_args()

    conn = db_mod.get_connection(args.db)

    current_year = date.today().year
    all_rows = []

    print(f"Fetching {current_year}...")
    all_rows.extend(fetch_csv(current_year))

    # Always fetch previous year as fallback (current year may have no data yet)
    if not all_rows:
        print(f"No {current_year} data, falling back to {current_year - 1}...")
        all_rows.extend(fetch_csv(current_year - 1))

    if not all_rows:
        print("No data fetched.")
        conn.close()
        return

    count = db_mod.upsert_treasury_yields(conn, all_rows)
    print(f"Upserted {count} yield curve rows.")

    # Print latest snapshot
    latest = db_mod.get_latest_yields(conn)
    if latest:
        print(f"\nLatest date: {latest['date']}")
        print("Curve:")
        for name, days in db_mod._TENORS:
            val = latest.get(name)
            label = name.replace("m", "").replace("y", "") + ("M" if "m" in name else "Y")
            print(f"  {label:>4s}: {val:.2f}%" if val is not None else f"  {label:>4s}: N/A")

    conn.close()


if __name__ == "__main__":
    main()
