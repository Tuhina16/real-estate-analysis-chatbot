import pandas as pd
import requests
from io import StringIO
import re

SHEET_CSV_URL = "https://docs.google.com/spreadsheets/d/1BPFvRBLAFFLyQ1EDJ4ogXt8HYCUXhM80/export?format=csv&gid=240339127"

_cached_df = None


def load_sheet_df(force=False):
    """
    Downloads Google Sheet CSV → returns DataFrame.
    Caches result for speed.
    """
    global _cached_df
    if _cached_df is None or force:
        r = requests.get(SHEET_CSV_URL)
        s = StringIO(r.text)
        df = pd.read_csv(s)

        df.columns = [c.strip() for c in df.columns]  # clean column names
        _cached_df = df
    return _cached_df


def extract_locations_from_query(query, df):
    """
    Detects 1–2 locations inside the user's natural-language query.
    Matches against the 'final location' column in the sheet.
    """

    query_lower = query.lower()

    location_columns = [
        col for col in df.columns
        if "location" in col.lower() or "area" in col.lower() or "locality" in col.lower()
    ]

    if not location_columns:
        return []

    loc_col = location_columns[0]

    unique_locations = df[loc_col].dropna().unique()

    matched = []
    for loc in unique_locations:
        if isinstance(loc, str) and loc.lower() in query_lower:
            matched.append(loc)

    return matched[:2]


def filter_area(df, area_name):
    """
    Returns rows where the location column matches the given area.
    """
    possible_cols = [c for c in df.columns if
                     'area' in c.lower() or 'locality' in c.lower() or 'location' in c.lower()]

    if not possible_cols:
        return pd.DataFrame()

    mask = False
    for col in possible_cols:
        mask = mask | (df[col].astype(str).str.lower() == area_name.lower())

    return df[mask]


def extract_year_span(query):
    """
    Looks for phrases like:
    - 'last 3 years'
    - 'past 2 years'
    Returns integer X or None.
    """
    match = re.search(r"(last|past)\s+(\d+)\s+years?", query.lower())
    if match:
        return int(match.group(2))
    return None


def pick_metric_column(df_area, query):
    """
    Automatically finds the best numerical column to plot based on query context.
    """
    query = query.lower()

    price_keywords = ["price", "rate", "growth", "appreciation"]
    demand_keywords = ["demand", "trend", "sales", "sold"]

    if any(k in query for k in price_keywords):
        for col in df_area.columns:
            if "weighted average rate" in col.lower():
                return col

    if any(k in query for k in demand_keywords):
        for col in df_area.columns:
            if "total sold" in col.lower():
                return col

    for col in df_area.columns:
        if "weighted average rate" in col.lower():
            return col

    for col in df_area.columns:
        if pd.api.types.is_numeric_dtype(df_area[col]):
            return col

    return None
