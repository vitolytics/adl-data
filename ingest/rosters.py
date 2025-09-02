"""
Fetch and persist MFL rosters for all seasons since 2018.

Outputs (data/rosters):
- rosters_YYYY.csv                -> One-time per historical year; re-fetched only if league id changes or file missing
- rosters_YYYY.csv (current year) -> Appended daily with an 'as_of' column (ISO date string)
- rosters_current.csv             -> Overwritten each run with the latest current-season snapshot

Environment (.ENV in repo root):
- mfl_api_key
- mfl_league_id
- current_season

API example:
https://api.myfantasyleague.com/{year}/export?TYPE=rosters&L=...&APIKEY=...&JSON=1

Notes:
- We do NOT iterate weeks; we capture the season-level roster snapshot returned by the endpoint.
- Historical years are fetched once and cached; current year is refreshed each run.
"""

from __future__ import annotations

import os
from datetime import datetime, date
from typing import Any, Dict, Iterable, List

import pandas as pd
import requests


# -------------------- utils --------------------

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
            if '#' in v:
                v = v.split('#', 1)[0].strip()
            if (len(v) >= 2) and ((v[0] == v[-1]) and v[0] in ('"', "'")):
                v = v[1:-1]
            else:
                v = v.strip('"').strip("'")
            result[k] = v
    return result


def _safe_int(x: Any) -> int | None:
    try:
        sx = str(x).strip()
        return int(sx)
    except Exception:
        return None


def _safe_float(x: Any) -> float | None:
    try:
        sx = str(x).strip()
        if sx == '':
            return None
        return float(sx)
    except Exception:
        return None


class RateLimitError(Exception):
    def __init__(self, year: int, message: str = "Rate limited"):
        super().__init__(message)
        self.year = year
        self.message = message


def _coalesce_attribute_stream(items: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Some MFL responses provide a stream of single-key dicts for players.
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


# -------------------- fetch/normalize --------------------

def fetch_rosters(year: int | str, league_id: str, api_key: str = "") -> Dict[str, Any]:
    """Call MFL rosters endpoint and return parsed JSON."""
    url = f"https://api.myfantasyleague.com/{year}/export"
    params = {
        'TYPE': 'rosters',
        'L': league_id,
        'JSON': '1',
    }
    if api_key:
        params['APIKEY'] = api_key

    last_err = None
    for attempt in range(3):
        try:
            resp = requests.get(url, params=params, timeout=25)
            if resp.status_code == 429:
                msg = f"HTTP 429 Too Many Requests at season {year}. Stopping so you can resume from here."
                print(msg)
                raise RateLimitError(int(year), msg)
            resp.raise_for_status()
            return resp.json()
        except requests.RequestException as e:
            last_err = e
    raise last_err


def normalize_rosters(data: Dict[str, Any], season: int | None = None) -> pd.DataFrame:
    """Normalize rosters JSON to a flat DataFrame.

    Columns:
    - season
    - week (if present in payload)
    - franchise_id
    - player_id
    - plus any attributes provided for players (status, salary, contractYear, contractInfo, etc.)
    """
    root = data.get('rosters', {})
    franchises = _to_list(root.get('franchise'))

    rows: List[Dict[str, Any]] = []

    for fr in franchises:
        if not isinstance(fr, dict):
            continue
        fid = str(fr.get('id')) if fr.get('id') is not None else None
        fweek = _safe_int(fr.get('week'))
        players = _to_list(fr.get('player'))
        if not players:
            continue

        # Detect and coalesce single-key stream
        if players and all(isinstance(p, dict) and len(p.keys()) <= 2 for p in players):
            players = _coalesce_attribute_stream(players)

        for p in players:
            if not isinstance(p, dict):
                continue
            row: Dict[str, Any] = {
                'season': int(season) if season is not None else None,
                'week': fweek,
                'franchise_id': fid,
                'player_id': str(p.get('id')) if p.get('id') is not None else None,
            }
            # Copy all attributes except 'id' (renamed to player_id above)
            for k, v in p.items():
                if k == 'id':
                    continue
                row[k] = v
            rows.append(row)

    if not rows:
        return pd.DataFrame()

    df = pd.DataFrame(rows)

    # Basic type cleanup
    if 'week' in df.columns:
        df['week'] = df['week'].astype('Int64')
    if 'season' in df.columns:
        df['season'] = df['season'].astype('Int64')
    if 'salary' in df.columns:
        df['salary'] = df['salary'].apply(_safe_float)
    if 'contractYear' in df.columns:
        df['contractYear'] = df['contractYear'].apply(_safe_int).astype('Int64')

    # Order columns
    preferred = ['season', 'week', 'franchise_id', 'player_id']
    other_cols = [c for c in df.columns if c not in preferred]
    df = df[preferred + other_cols]
    return df


# -------------------- saving --------------------

def _rosters_dir() -> str:
    d = os.path.join(_repo_root(), 'data', 'rosters')
    _ensure_dir(d)
    return d


def save_historical_year(df: pd.DataFrame, year: int) -> str:
    out = os.path.join(_rosters_dir(), f'rosters_{year}.csv')
    df = df.copy()
    df['season'] = int(year)
    df.to_csv(out, index=False)
    return out


def save_current_snapshot(df: pd.DataFrame, season: int, as_of: str) -> str:
    out = os.path.join(_rosters_dir(), 'rosters_current.csv')
    df = df.copy()
    df['season'] = int(season)
    df['as_of'] = as_of
    df.to_csv(out, index=False)
    return out


def append_current_year_timeseries(df: pd.DataFrame, season: int, as_of: str) -> str:
    out = os.path.join(_rosters_dir(), f'rosters_{season}.csv')
    df = df.copy()
    df['season'] = int(season)
    df['as_of'] = as_of

    if os.path.exists(out):
        try:
            existing = pd.read_csv(out)
            combined = pd.concat([existing, df], ignore_index=True, sort=False)
            # De-dupe by snapshot day + franchise + player
            if {'as_of', 'franchise_id', 'player_id'}.issubset(combined.columns):
                combined.drop_duplicates(subset=['as_of', 'franchise_id', 'player_id'], keep='last', inplace=True)
            combined.to_csv(out, index=False)
        except Exception:
            # If read fails, fall back to writing fresh
            df.to_csv(out, index=False)
    else:
        df.to_csv(out, index=False)
    return out


# -------------------- orchestration --------------------

def _league_id_cache_path() -> str:
    return os.path.join(_rosters_dir(), '_league_id.txt')


def _read_cached_league_id() -> str | None:
    p = _league_id_cache_path()
    if not os.path.exists(p):
        return None
    try:
        with open(p, 'r', encoding='utf-8') as f:
            return f.read().strip() or None
    except Exception:
        return None


def _write_cached_league_id(league_id: str) -> None:
    p = _league_id_cache_path()
    with open(p, 'w', encoding='utf-8') as f:
        f.write(str(league_id))


def _years_to_process(current_season: int) -> List[int]:
    # Mirror other ingesters: start at 2018
    start_year = 2018
    return list(range(start_year, current_season))  # historical only (exclude current)


def run(league_id: str, api_key: str, current_season: int) -> None:
    cached_lid = _read_cached_league_id()
    lid_changed = (cached_lid != str(league_id))
    if lid_changed:
        print(f"League ID changed or not cached (was: {cached_lid}); historical years will be re-fetched.")

    # Historical seasons: fetch if missing or league id changed
    for y in _years_to_process(current_season):
        out_path = os.path.join(_rosters_dir(), f'rosters_{y}.csv')
        if not lid_changed and os.path.exists(out_path):
            print(f"Historical {y} exists; skipping.")
            continue
        print(f"Fetching rosters for {y}...")
        try:
            data = fetch_rosters(y, league_id, api_key)
            df = normalize_rosters(data, season=y)
            if df is None or len(df) == 0:
                print(f"  no data returned; creating empty file")
                pd.DataFrame(columns=['season', 'week', 'franchise_id', 'player_id']).to_csv(out_path, index=False)
            else:
                save_historical_year(df, y)
                print(f"  saved: {out_path} ({len(df)} rows)")
        except RateLimitError as rle:
            print(f"Rate limit encountered at season {rle.year}. Exiting early.")
            return
        except Exception as e:
            print(f"Error processing season {y}: {e}")

    # Current season: always refresh snapshot and append to time series with as_of
    today = date.today().isoformat()
    print(f"Fetching current season {current_season} rosters...")
    try:
        data = fetch_rosters(current_season, league_id, api_key)
        df = normalize_rosters(data, season=current_season)
        if df is None or len(df) == 0:
            print("  no data returned for current season.")
        else:
            snap = save_current_snapshot(df, current_season, today)
            roll = append_current_year_timeseries(df, current_season, today)
            print(f"  saved snapshot: {snap}")
            print(f"  updated timeseries: {roll}")
    except RateLimitError as rle:
        print(f"Rate limit encountered at season {rle.year}. Exiting early.")
        return
    except Exception as e:
        print(f"Error processing current season: {e}")

    # Update cache after a successful run
    _write_cached_league_id(league_id)


if __name__ == '__main__':
    root = _repo_root()
    env = _load_env(os.path.join(root, '.ENV'))
    league_id = env.get('mfl_league_id') or env.get('MFL_LEAGUE_ID') or '60206'
    api_key = env.get('mfl_api_key') or env.get('MFL_API_KEY') or ''
    current_season = int((env.get('current_season') or env.get('CURRENT_SEASON') or '2025').strip())

    run(league_id=league_id, api_key=api_key, current_season=current_season)
