"""
Fetches MFL salary adjustments for a range of seasons and writes one CSV per season.

Rules:
- Historical years: data/salaryAdjustments/salaryAdjustment_{YEAR}_seasonEnd.csv
- Current year (from .ENV current_season): data/salaryAdjustments/salaryAdjustment_{YEAR}_asof_{mm dd yyyy hh mm}.csv

Environment (.ENV in repo root):
- mfl_api_key
- mfl_league_id
- current_season

API example: https://www46.myfantasyleague.com/2025/export?TYPE=salaryAdjustments&L=60206&APIKEY=&JSON=1
"""

from __future__ import annotations

import os
import time
from datetime import datetime, timezone
from typing import Any, Dict, Iterable, List

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
            v = v.strip().strip('"').strip("'")
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


def fetch_salary_adjustments(year: int | str, league_id: str, api_key: str = "") -> Dict[str, Any]:
    """
    Call MFL salaryAdjustments endpoint and return parsed JSON.
    """
    # Mirror example host
    url = f"https://www46.myfantasyleague.com/{year}/export"
    params = {
        'TYPE': 'salaryAdjustments',
        'L': league_id,
        'APIKEY': api_key,
        'JSON': '1',
    }

    last_err = None
    for attempt in range(3):
        try:
            resp = requests.get(url, params=params, timeout=25)
            resp.raise_for_status()
            return resp.json()
        except requests.RequestException as e:
            last_err = e
            time.sleep(0.5 * (attempt + 1))
    raise last_err


def normalize_salary_adjustments(data: Dict[str, Any]) -> pd.DataFrame:
    """Normalize salaryAdjustments JSON to a flat DataFrame, one row per entry."""
    root = data.get('salaryAdjustments', {})
    items = _to_list(root.get('salaryAdjustment'))

    rows: List[Dict[str, Any]] = []
    for obj in items:
        if not isinstance(obj, dict):
            continue
        ts = _safe_int(obj.get('timestamp'))
        ts_iso = (
            datetime.fromtimestamp(ts, tz=timezone.utc).isoformat() if ts is not None else None
        )
        rows.append({
            'id': str(obj.get('id')) if obj.get('id') is not None else None,
            'franchise_id': str(obj.get('franchise_id')) if obj.get('franchise_id') is not None else None,
            'amount': _safe_float(obj.get('amount')),
            'description': obj.get('description'),
            'timestamp': ts,
            'ts_utc': ts_iso,
        })

    if not rows:
        return pd.DataFrame()

    df = pd.DataFrame(rows)
    return df


def save_salary_adjustments_csv(df: pd.DataFrame, year: int, is_current_year: bool, when: datetime | None = None) -> str:
    out_dir = os.path.join(_repo_root(), 'data', 'salaryAdjustments')
    _ensure_dir(out_dir)

    df = df.copy()
    df['season'] = int(year)
    cols = ['season'] + [c for c in df.columns if c != 'season']
    df = df[cols]

    if is_current_year:
        when = when or datetime.now()
        ts = when.strftime('%m %d %Y %H %M')
        # Note: per requirements, current-year filename uses "salaryAdjustment_" prefix
        filename = f"salaryAdjustment_{year}_asof_{ts}.csv"
    else:
        filename = f"salaryAdjustment_{year}_seasonEnd.csv"

    out_path = os.path.join(out_dir, filename)
    df.to_csv(out_path, index=False)
    return out_path


def process_season(year: int, league_id: str, api_key: str, current_year: int) -> pd.DataFrame:
    print(f"Processing salary adjustments for season {year}...")
    data = fetch_salary_adjustments(year, league_id, api_key)
    df = normalize_salary_adjustments(data)
    print(f"Fetched {len(df)} salary adjustment rows for {year}")
    out_path = save_salary_adjustments_csv(df, year, is_current_year=(year == current_year))
    print(f"Saved CSV: {out_path}")
    return df


def process_multiple_years(years: Iterable[int], league_id: str, api_key: str, current_year: int) -> None:
    years_list = list(years)
    for idx, y in enumerate(years_list):
        try:
            process_season(int(y), league_id, api_key, current_year)
        except Exception as e:
            print(f"Error processing year {y}: {e}")
        if idx < len(years_list) - 1:
            time.sleep(2)


if __name__ == '__main__':
    root = _repo_root()
    env = _load_env(os.path.join(root, '.ENV'))
    league_id = env.get('mfl_league_id') or env.get('MFL_LEAGUE_ID') or '60206'
    api_key = env.get('mfl_api_key') or env.get('MFL_API_KEY') or ''
    current_season = int((env.get('current_season') or env.get('CURRENT_SEASON') or '2025').strip())

    # Default range mirrors other ingestors: from 2018 through current season inclusive
    years_to_process = range(2018, current_season + 1)
    process_multiple_years(years_to_process, league_id, api_key, current_season)
