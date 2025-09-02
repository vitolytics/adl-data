"""
Fetches MFL player scores for a range of seasons and weeks and writes one CSV per week plus a yearly CSV.

Rules:
- Weekly files: data/playerScores/playerScores_{YEAR}_w{WEEK}.csv
- Yearly file (combined across processed weeks): data/playerScores/playerScores_{YEAR}.csv
- Weeks processed: 1–18 for all years (skips weeks that return no data)
- Current season is capped by .ENV current_week (if provided)
- 5-second pause between week requests

Environment (.ENV in repo root):
- mfl_api_key
- mfl_league_id
- current_season
- current_week

API host: https://api.myfantasyleague.com/{year}/export?TYPE=playerScores&L=...&W=...&JSON=1
"""

from __future__ import annotations

import os
import time
from datetime import datetime
from typing import Any, Dict, Iterable, List, Sequence

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
class RateLimitError(Exception):
    def __init__(self, year: int, week: int, message: str = "Rate limited"):
        super().__init__(message)
        self.year = year
        self.week = week
        self.message = message



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
            # Explicit rate-limit handling
            if resp.status_code == 429:
                msg = f"HTTP 429 Too Many Requests at season {year} week {week}. Stopping so you can resume from here."
                print(msg)
                raise RateLimitError(int(year), int(week), msg)
            resp.raise_for_status()
            return resp.json()
        except requests.RequestException as e:
            last_err = e
            time.sleep(0.5 * (attempt + 1))
    raise last_err


def _coalesce_attribute_stream(items: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Some MFL responses (sample included) provide playerScore as a stream of single-key dicts.
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
    out_dir = os.path.join(_repo_root(), 'data', 'playerScores')
    _ensure_dir(out_dir)
    df = df.copy()
    # Add season/week to weekly files as well (useful downstream)
    df['season'] = int(year)
    df['week'] = int(week)
    cols = ['season', 'week'] + [c for c in df.columns if c not in ('season', 'week')]
    df = df[cols]
    out_path = os.path.join(out_dir, f'playerScores_{year}_w{int(week)}.csv')
    df.to_csv(out_path, index=False)
    return out_path


def save_yearly_csv(frames: Sequence[pd.DataFrame], year: int) -> str:
    out_dir = os.path.join(_repo_root(), 'data', 'playerScores')
    _ensure_dir(out_dir)
    if not frames:
        # Create an empty file with headers
        empty = pd.DataFrame(columns=['season', 'week'])
        out_path = os.path.join(out_dir, f'playerScores_{year}.csv')
        empty.to_csv(out_path, index=False)
        return out_path
    combined = pd.concat(frames, ignore_index=True, sort=False)
    # Ensure season/week columns present
    combined['season'] = int(year)
    if 'week' not in combined.columns:
        combined['week'] = pd.NA
    cols = ['season', 'week'] + [c for c in combined.columns if c not in ('season', 'week')]
    combined = combined[cols]
    out_path = os.path.join(out_dir, f'playerScores_{year}.csv')
    combined.to_csv(out_path, index=False)
    return out_path


def process_weeks_for_year(year: int, weeks: Iterable[int], league_id: str, api_key: str) -> List[pd.DataFrame]:
    weekly_frames: List[pd.DataFrame] = []
    weeks_list = list(weeks)
    for idx, w in enumerate(weeks_list):
        try:
            print(f"Fetching player scores for {year} week {w}...")
            data = fetch_player_scores(year, w, league_id, api_key)
            df = normalize_player_scores(data, fallback_week=w)
            if df is None or len(df) == 0:
                print("  no data returned; skipping")
            else:
                print(f"  rows: {len(df)}")
                save_weekly_csv(df, year, w)
                weekly_frames.append(df)
        except RateLimitError as rle:
            # Surface a clear message and stop further processing
            print(f"Rate limit encountered at season {rle.year} week {rle.week}. Exiting early.")
            raise
        except Exception as e:
            print(f"Error processing {year} week {w}: {e}")
        # 5-second pacing between weeks (not after the last one)
        if idx < len(weeks_list) - 1:
            time.sleep(5)
    return weekly_frames


def _default_weeks_for_year(year: int, current_year: int, current_week: int | None) -> List[int]:
    # Historical: always weeks 1–18
    if year < current_year:
        return list(range(1, 19))
    # Current season: cap at current_week if provided, else 18
    cap = current_week if current_week is not None else 18
    cap = max(1, min(int(cap), 18))
    return list(range(1, cap + 1))


if __name__ == '__main__':
    root = _repo_root()
    env = _load_env(os.path.join(root, '.ENV'))
    league_id = env.get('mfl_league_id') or env.get('MFL_LEAGUE_ID') or '60206'
    api_key = env.get('mfl_api_key') or env.get('MFL_API_KEY') or ''
    current_season = int((env.get('current_season') or env.get('CURRENT_SEASON') or '2025').strip())
    cw_raw = (env.get('current_week') or env.get('CURRENT_WEEK') or '').strip()
    try:
        current_week = int(cw_raw) if cw_raw else None
    except Exception:
        current_week = None

    # Years to process: from 2018 through current season inclusive (mirroring other ingesters)
    years_to_process = list(range(2018, current_season + 1))

    for y in years_to_process:
        try:
            weeks = _default_weeks_for_year(y, current_season, current_week)
            weekly_frames = process_weeks_for_year(y, weeks, league_id, api_key)
            out_path = save_yearly_csv(weekly_frames, y)
            print(f"Saved yearly CSV: {out_path}")
        except RateLimitError as rle:
            print(f"Stopped due to rate limiting at season {rle.year} week {rle.week}. You can resume from this point.")
            break
