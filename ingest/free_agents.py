"""
Fetches free agents for the current season and writes a timestamped CSV.

Reads config from .ENV (mfl_api_key, mfl_league_id, current_season).
Output: data/freeAgents/freeAgents_{mmddyyyy hhmm}.csv
"""

from __future__ import annotations

import os
import json
import time
from datetime import datetime
from typing import Any, Dict, List

import requests
import pandas as pd


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


def fetch_free_agents(year: int | str, league_id: str, api_key: str = "", position: str = "") -> Dict[str, Any]:
    """
    Call MFL freeAgents endpoint and return parsed JSON.
    Uses the stable api.myfantasyleague.com host similar to leagues ingest.
    """
    url = f"https://api.myfantasyleague.com/{year}/export"
    params = {
        'TYPE': 'freeAgents',
        'L': league_id,
        'APIKEY': api_key,
        'POSITION': position,
        'JSON': '1',
    }

    last_err = None
    for attempt in range(3):
        try:
            resp = requests.get(url, params=params, timeout=20)
            resp.raise_for_status()
            return resp.json()
        except requests.RequestException as e:
            last_err = e
            time.sleep(0.5 * (attempt + 1))
    raise last_err


def _coalesce_attribute_stream(items: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Some MFL responses can (rarely) present players as a stream of single-key dicts.
    Attempt to coalesce them into records. Heuristic: start a new record when an 'id'
    key is encountered and the current record already has any data. Otherwise, keep
    appending attributes to the current record. Flush the final record at the end.
    """
    rows: List[Dict[str, Any]] = []
    current: Dict[str, Any] = {}
    for obj in items:
        if not isinstance(obj, dict):
            continue
        keys = list(obj.keys())
        # If this entry looks like a full player (multi-key), treat directly
        if len(keys) > 1:
            if current:
                rows.append(current)
                current = {}
            rows.append(obj)
            continue
        # Single-key dict; merge into current, separate by id boundary
        k = keys[0] if keys else None
        if k == 'id' and current:
            rows.append(current)
            current = {}
        if k is not None:
            current[k] = obj[k]
    if current:
        rows.append(current)
    return rows


def normalize_free_agents(data: Dict[str, Any]) -> pd.DataFrame:
    """Normalize free agents JSON to a flat DataFrame, one row per player."""
    fa = data.get('freeAgents', {})
    units = _to_list(fa.get('leagueUnit'))
    all_rows: List[Dict[str, Any]] = []

    for unit in units:
        unit_name = unit.get('unit') if isinstance(unit, dict) else None
        players = []
        if isinstance(unit, dict):
            players = unit.get('player')
        players = _to_list(players)

        # Detect if we have a stream of single-key dicts vs normal dict-per-player
        is_stream = players and all(isinstance(p, dict) and len(p.keys()) <= 2 for p in players)

        if is_stream:
            coalesced = _coalesce_attribute_stream(players)
            for p in coalesced:
                row = dict(p)
                if unit_name is not None:
                    # store as conference (will coerce to int later)
                    row['conference'] = unit_name
                all_rows.append(row)
        else:
            for p in players:
                if isinstance(p, dict):
                    row = dict(p)
                    if unit_name is not None:
                        row['conference'] = unit_name
                    all_rows.append(row)

    if not all_rows:
        return pd.DataFrame()

    # Build DataFrame and ensure consistent columns
    df = pd.DataFrame(all_rows)
    # Convert conference like "CONFERENCE00" to integer 0/1
    if 'conference' in df.columns:
        def _conf_to_int(v: Any):
            if v is None:
                return None
            s = str(v).strip().upper()
            if s.startswith('CONFERENCE'):
                s = s[len('CONFERENCE'):]
            # keep only digits
            s = ''.join(ch for ch in s if ch.isdigit())
            if s == '':
                return None
            try:
                return int(s)
            except Exception:
                return None
        df['conference'] = df['conference'].map(_conf_to_int)

    # Basic cleanup: ensure key text fields are strings
    for col in [c for c in ['id', 'name', 'position', 'team', 'status', 'contractYear', 'contractInfo', 'salary'] if c in df.columns]:
        df[col] = df[col].astype(str)

    # Remove any lingering 'unit' column if present
    if 'unit' in df.columns:
        df = df.drop(columns=['unit'])
    return df


def save_free_agents_csv(df: pd.DataFrame, when: datetime) -> str:
    out_dir = os.path.join(_repo_root(), 'data', 'freeAgents')
    _ensure_dir(out_dir)
    ts = when.strftime('%m%d%Y %H%M')
    out_path = os.path.join(out_dir, f'freeAgents_{ts}.csv')
    df.to_csv(out_path, index=False)
    return out_path


def main():
    root = _repo_root()
    env = _load_env(os.path.join(root, '.ENV'))
    year = env.get('current_season') or env.get('CURRENT_SEASON') or '2025'
    league_id = env.get('mfl_league_id') or env.get('MFL_LEAGUE_ID') or '60206'
    api_key = env.get('mfl_api_key') or env.get('MFL_API_KEY') or ''

    print(f"Fetching free agents for season {year}, league {league_id}...")
    data = fetch_free_agents(year, league_id, api_key)
    df = normalize_free_agents(data)
    print(f"Free agents fetched: {len(df)} rows")

    out_path = save_free_agents_csv(df, datetime.now())
    print(f"Saved CSV: {out_path}")


if __name__ == '__main__':
    main()
