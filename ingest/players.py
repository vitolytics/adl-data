"""
Fetches MFL players for a range of seasons and writes one CSV per year plus a combined CSV.

Rules:
- Yearly files: data/players/player_{YEAR}.csv (includes a 'year' column)
- Combined file: data/players/players_all.csv (includes a 'year' column)
- Years processed: 2018 through current season (from .ENV)
- 2-second pause between year requests

Environment (.ENV in repo root):
- mfl_api_key
- mfl_league_id
- current_season

API host: https://api.myfantasyleague.com/{year}/export?TYPE=players&L=...&DETAILS=1&JSON=1
"""

from __future__ import annotations

import os
import time
from typing import Any, Dict, Iterable, List, Sequence

import pandas as pd
import requests


# -----------------------------
# Utilities (mirroring other ingesters)
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


class RateLimitError(Exception):
    def __init__(self, year: int, message: str = "Rate limited"):
        super().__init__(message)
        self.year = year
        self.message = message


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
            if resp.status_code == 429:
                msg = f"HTTP 429 Too Many Requests at season {year}. Stopping so you can resume from here."
                print(msg)
                raise RateLimitError(int(year), msg)
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
    return out_path


def save_combined_csv(frames: Sequence[pd.DataFrame]) -> str:
    out_dir = os.path.join(_repo_root(), 'data', 'players')
    _ensure_dir(out_dir)
    if not frames:
        # Create an empty combined file with just 'year' column
        empty = pd.DataFrame(columns=['year'])
        out_path = os.path.join(out_dir, 'players_all.csv')
        empty.to_csv(out_path, index=False)
        return out_path
    combined = pd.concat(frames, ignore_index=True, sort=False)
    if 'year' not in combined.columns:
        combined['year'] = pd.NA
    cols = ['year'] + [c for c in combined.columns if c != 'year']
    combined = combined[cols]
    out_path = os.path.join(out_dir, 'players_all.csv')
    combined.to_csv(out_path, index=False)
    return out_path


# -----------------------------
# Orchestration
# -----------------------------

def process_year(year: int, league_id: str, api_key: str) -> pd.DataFrame:
    print(f"Fetching players for {year}...")
    data = fetch_players(year, league_id, api_key)
    df = normalize_players(data)
    if df is None or len(df) == 0:
        print("  no data returned; skipping save")
        return pd.DataFrame()
    print(f"  rows: {len(df)}")
    save_year_csv(df, year)
    return df


if __name__ == '__main__':
    root = _repo_root()
    env = _load_env(os.path.join(root, '.ENV'))
    league_id = env.get('mfl_league_id') or env.get('MFL_LEAGUE_ID') or '60206'
    api_key = env.get('mfl_api_key') or env.get('MFL_API_KEY') or ''
    current_season = int((env.get('current_season') or env.get('CURRENT_SEASON') or '2025').strip())

    years_to_process: Iterable[int] = range(2018, current_season + 1)

    frames: List[pd.DataFrame] = []
    try:
        for idx, y in enumerate(years_to_process):
            try:
                df = process_year(y, league_id, api_key)
                if not df.empty:
                    frames.append(df.assign(year=int(y)))
            except RateLimitError as rle:
                print(f"Rate limit encountered at season {rle.year}. Exiting early.")
                break
            except Exception as e:
                print(f"Error processing {y}: {e}")
            # gentle pacing between years (not after the last one)
            if idx < (len(list(years_to_process)) - 1):
                time.sleep(2)
    finally:
        combined_path = save_combined_csv(frames)
        print(f"Saved combined players CSV: {combined_path}")
