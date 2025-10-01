"""
Fetches MFL player scores for the current season and past week only.
This is designed to be run weekly to keep the current season data fresh.

Rules:
- Only processes the current season as defined in .ENV
- Automatically detects the current week from ESPN's public API
- Updates weekly file: data/playerScores/playerScores_{YEAR}_w{WEEK}.csv
- Updates yearly file: data/playerScores/playerScores_{YEAR}.csv (appends/updates the week)
- 5-second pause between requests

Environment (.ENV in repo root):
- mfl_api_key
- mfl_league_id
- current_season

API hosts:
- ESPN Week API: https://site.api.espn.com/apis/site/v2/sports/football/nfl/scoreboard
- MFL Scores API: https://api.myfantasyleague.com/{year}/export?TYPE=playerScores&L=...&W=...&JSON=1
"""

from __future__ import annotations

import os
import time
from datetime import datetime
from typing import Any, Dict, List

import pandas as pd
import requests


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


def get_current_nfl_week() -> int:
    """
    Fetch the current NFL week from ESPN's public API.
    Returns the week number (1-18 for regular season).
    Falls back to week 1 if there's an error.
    """
    try:
        # Try ESPN API first (public, no auth required)
        url = "https://site.api.espn.com/apis/site/v2/sports/football/nfl/scoreboard"
        
        print(f"Fetching current NFL week from ESPN API...")
        resp = requests.get(url, timeout=10)
        resp.raise_for_status()
        data = resp.json()
        
        # ESPN API returns current week info
        week = data.get('week', {}).get('number')
        season_type = data.get('season', {}).get('type')
        
        if week is not None:
            print(f"  Current NFL week: {week} (season type: {season_type})")
            return int(week)
        else:
            print("  Warning: ESPN API did not return a week number, defaulting to week 1")
            return 1
            
    except Exception as e:
        print(f"  Error fetching NFL week from ESPN: {e}")
        
        # Try alternative: NFL.com public calendar API
        try:
            today = datetime.now().strftime('%Y-%m-%d')
            url = f"https://api.nfl.com/v1/calendar/weeks/{today}"
            print(f"  Trying NFL.com API...")
            resp = requests.get(url, timeout=10)
            resp.raise_for_status()
            data = resp.json()
            week = data.get('week')
            if week:
                print(f"  Current NFL week from NFL.com: {week}")
                return int(week)
        except Exception as e2:
            print(f"  Error with NFL.com API: {e2}")
        
        print("  Defaulting to week 1")
        return 1


def fetch_player_scores(year: int | str, week: int | str, league_id: str, api_key: str = "") -> Dict[str, Any]:
    """Call MFL playerScores endpoint and return parsed JSON."""
    url = f"https://api.myfantasyleague.com/{year}/export"
    params = {
        'TYPE': 'playerScores',
        'L': league_id,
        'APIKEY': api_key,
        'W': str(week),
        'JSON': '1',
    }

    last_err = None
    for attempt in range(3):
        try:
            resp = requests.get(url, params=params, timeout=25)
            if resp.status_code == 429:
                msg = f"HTTP 429 Too Many Requests at season {year} week {week}."
                print(msg)
                raise Exception(msg)
            resp.raise_for_status()
            return resp.json()
        except requests.RequestException as e:
            last_err = e
            time.sleep(0.5 * (attempt + 1))
    raise last_err


def _coalesce_attribute_stream(items: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Some MFL responses provide playerScore as a stream of single-key dicts.
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


def normalize_player_scores(data: Dict[str, Any], fallback_week: int | None = None) -> pd.DataFrame:
    """Normalize playerScores JSON to a flat DataFrame, one row per player/week."""
    root = data.get('playerScores', {})
    items = _to_list(root.get('playerScore'))

    rows: List[Dict[str, Any]] = []

    if items and all(isinstance(p, dict) and len(p.keys()) <= 2 for p in items):
        # Coalesce single-key stream
        items = _coalesce_attribute_stream(items)

    for obj in items:
        if not isinstance(obj, dict):
            continue
        row: Dict[str, Any] = {
            'id': str(obj.get('id')) if obj.get('id') is not None else None,
            'week': _safe_int(obj.get('week')) or fallback_week,
            'score': _safe_float(obj.get('score')),
        }
        # Include isAvailable if present
        if 'isAvailable' in obj:
            row['isAvailable'] = _safe_int(obj.get('isAvailable'))
        rows.append(row)

    if not rows:
        return pd.DataFrame()

    df = pd.DataFrame(rows)
    # Ensure dtypes
    if 'week' in df.columns:
        df['week'] = df['week'].astype('Int64')
    return df


def save_weekly_csv(df: pd.DataFrame, year: int, week: int) -> str:
    """Save the weekly player scores CSV."""
    out_dir = os.path.join(_repo_root(), 'data', 'playerScores')
    _ensure_dir(out_dir)
    df = df.copy()
    # Add season/week to weekly files
    df['season'] = int(year)
    df['week'] = int(week)
    cols = ['season', 'week'] + [c for c in df.columns if c not in ('season', 'week')]
    df = df[cols]
    out_path = os.path.join(out_dir, f'playerScores_{year}_w{int(week)}.csv')
    df.to_csv(out_path, index=False)
    print(f"Saved weekly CSV: {out_path}")
    return out_path


def update_yearly_csv(new_week_df: pd.DataFrame, year: int, week: int) -> str:
    """Update the yearly CSV by removing old data for this week and adding the new data."""
    out_dir = os.path.join(_repo_root(), 'data', 'playerScores')
    _ensure_dir(out_dir)
    out_path = os.path.join(out_dir, f'playerScores_{year}.csv')
    
    # Prepare the new week data with season/week columns
    new_df = new_week_df.copy()
    new_df['season'] = int(year)
    new_df['week'] = int(week)
    
    # Load existing yearly file if it exists
    if os.path.exists(out_path):
        existing = pd.read_csv(out_path)
        # Remove any existing data for this week
        existing = existing[existing['week'] != week]
        # Combine with new data
        combined = pd.concat([existing, new_df], ignore_index=True, sort=False)
    else:
        combined = new_df
    
    # Ensure column order
    cols = ['season', 'week'] + [c for c in combined.columns if c not in ('season', 'week')]
    combined = combined[cols]
    
    # Sort by week for readability
    if 'week' in combined.columns:
        combined = combined.sort_values(['week', 'id'] if 'id' in combined.columns else ['week'])
    
    combined.to_csv(out_path, index=False)
    print(f"Updated yearly CSV: {out_path}")
    return out_path


if __name__ == '__main__':
    root = _repo_root()
    env = _load_env(os.path.join(root, '.ENV'))
    
    league_id = env.get('mfl_league_id') or env.get('MFL_LEAGUE_ID') or '60206'
    api_key = env.get('mfl_api_key') or env.get('MFL_API_KEY') or ''
    current_season = int((env.get('current_season') or env.get('CURRENT_SEASON') or '2025').strip())
    
    # Automatically detect the current week from NFL API
    current_week = get_current_nfl_week()
    
    print(f"\nFetching player scores for season {current_season}, week {current_week}...")
    
    try:
        # Fetch data for the current week
        data = fetch_player_scores(current_season, current_week, league_id, api_key)
        df = normalize_player_scores(data, fallback_week=current_week)
        
        if df is None or len(df) == 0:
            print("No data returned for this week.")
        else:
            print(f"Retrieved {len(df)} player score records")
            
            # Save weekly CSV
            save_weekly_csv(df, current_season, current_week)
            
            # Update yearly CSV
            update_yearly_csv(df, current_season, current_week)
            
            print("Successfully updated player scores for the current week!")
            
    except Exception as e:
        print(f"Error fetching player scores: {e}")
        raise
