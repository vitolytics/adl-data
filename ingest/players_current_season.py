"""
Fetches MFL players for ONLY the current season and updates the relevant CSV files.

This is a lightweight version of players.py optimized for scheduled updates.
It updates:
- data/players/player_{CURRENT_YEAR}.csv
- data/players/players_all.csv (by appending/updating current year data)

Environment (.ENV in repo root):
- mfl_api_key
- mfl_league_id
- current_season
"""

from __future__ import annotations

import os
import time
from typing import Any, Dict, List

import pandas as pd
import requests


# -----------------------------
# Utilities
# -----------------------------

def _repo_root() -> str:
    return os.path.abspath(os.path.join(os.path.dirname(__file__), os.pardir))


def _ensure_dir(p: str) -> None:
    os.makedirs(p, exist_ok=True)


def _to_list(maybe_list):
    if maybe_list is None:
        return []
    if isinstance(maybe_list, list):
        return maybe_list
    return [maybe_list]


def _load_env(env_path: str) -> Dict[str, str]:
    """Load simple KEY = 'VAL' lines from .ENV without extra deps."""
    result: Dict[str, str] = {}
    if not os.path.exists(env_path):
        return result
    with open(env_path, 'r', encoding='utf-8') as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith('#'):
                continue
            if '=' not in line:
                continue
            k, v = line.split('=', 1)
            k = k.strip()
            v = v.strip()
            # Remove inline comments first, then strip surrounding quotes
            if '#' in v:
                v = v.split('#', 1)[0].strip()
            # Now remove wrapping quotes if present
            if (len(v) >= 2) and ((v[0] == v[-1]) and v[0] in ('"', "'")):
                v = v[1:-1]
            else:
                v = v.strip('"').strip("'")
            result[k] = v
    return result


# -----------------------------
# HTTP
# -----------------------------

def fetch_players(year: int | str, league_id: str, api_key: str = "") -> Dict[str, Any]:
    """Call MFL players endpoint and return parsed JSON."""
    url = f"https://api.myfantasyleague.com/{year}/export"
    params = {
        'TYPE': 'players',
        'L': league_id,
        'APIKEY': api_key,
        'DETAILS': '1',
        'JSON': '1',
    }

    last_err = None
    for attempt in range(3):
        try:
            resp = requests.get(url, params=params, timeout=30)
            resp.raise_for_status()
            return resp.json()
        except requests.RequestException as e:
            last_err = e
            time.sleep(0.5 * (attempt + 1))
    if last_err:
        raise last_err
    return {}


# -----------------------------
# Normalization
# -----------------------------

def _coalesce_attribute_stream(items: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Some MFL responses provide players as a stream of single-key dicts.
    Coalesce such streams into one dict per player, starting a new record on 'id' boundaries.
    """
    rows: List[Dict[str, Any]] = []
    current: Dict[str, Any] = {}
    for obj in items:
        if not isinstance(obj, dict):
            continue
        keys = list(obj.keys())
        if len(keys) > 1:
            if current:
                rows.append(current)
                current = {}
            rows.append(obj)
            continue
        k = keys[0] if keys else None
        if k == 'id' and current:
            rows.append(current)
            current = {}
        if k is not None:
            current[k] = obj[k]
    if current:
        rows.append(current)
    return rows


def normalize_players(data: Dict[str, Any]) -> pd.DataFrame:
    """Normalize players JSON to a flat DataFrame, one row per player."""
    root = data.get('players', {})
    items = _to_list(root.get('player'))

    if not items:
        return pd.DataFrame()

    # If it's a stream of single-key dicts, coalesce to proper records
    if items and all(isinstance(p, dict) and len(p.keys()) <= 2 for p in items):
        items = _coalesce_attribute_stream(items)

    # Build rows keeping all keys; keep values as strings for stability
    rows: List[Dict[str, Any]] = []
    for obj in items:
        if not isinstance(obj, dict):
            continue
        # Force id to string if present
        row = {k: (str(v) if v is not None else None) for k, v in obj.items()}
        rows.append(row)

    if not rows:
        return pd.DataFrame()

    df = pd.DataFrame(rows)
    return df


# -----------------------------
# Persistence
# -----------------------------

def save_year_csv(df: pd.DataFrame, year: int) -> str:
    out_dir = os.path.join(_repo_root(), 'data', 'players')
    _ensure_dir(out_dir)
    df = df.copy()
    df['year'] = int(year)
    cols = ['year'] + [c for c in df.columns if c != 'year']
    df = df[cols]
    out_path = os.path.join(out_dir, f'player_{year}.csv')
    df.to_csv(out_path, index=False)
    print(f"  Saved: {out_path}")
    return out_path


def update_combined_csv(current_year_df: pd.DataFrame, current_year: int) -> str:
    """Update the combined CSV with the current year's data."""
    out_dir = os.path.join(_repo_root(), 'data', 'players')
    _ensure_dir(out_dir)
    combined_path = os.path.join(out_dir, 'players_all.csv')
    
    # Read existing combined file
    if os.path.exists(combined_path):
        existing_df = pd.read_csv(combined_path)
        # Remove existing rows for current year
        existing_df = existing_df[existing_df['year'] != current_year]
        # Append current year data
        combined = pd.concat([existing_df, current_year_df], ignore_index=True, sort=False)
    else:
        combined = current_year_df.copy()
    
    # Ensure year column is first
    if 'year' in combined.columns:
        cols = ['year'] + [c for c in combined.columns if c != 'year']
        combined = combined[cols]
    
    combined.to_csv(combined_path, index=False)
    print(f"  Updated: {combined_path}")
    return combined_path


# -----------------------------
# Main
# -----------------------------

if __name__ == '__main__':
    root = _repo_root()
    env = _load_env(os.path.join(root, '.ENV'))
    
    league_id = env.get('mfl_league_id') or env.get('MFL_LEAGUE_ID') or '60206'
    api_key = env.get('mfl_api_key') or env.get('MFL_API_KEY') or ''
    current_season = int((env.get('current_season') or env.get('CURRENT_SEASON') or '2025').strip())
    
    print(f"Fetching players for {current_season} (current season only)...")
    data = fetch_players(current_season, league_id, api_key)
    df = normalize_players(data)
    
    if df is None or len(df) == 0:
        print("  No data returned; exiting")
        exit(1)
    
    print(f"  Rows retrieved: {len(df)}")
    
    # Add year column
    df['year'] = int(current_season)
    
    # Save individual year file
    save_year_csv(df, current_season)
    
    # Update combined file
    update_combined_csv(df, current_season)
    
    print(f"\n✓ Successfully updated players data for {current_season}")

