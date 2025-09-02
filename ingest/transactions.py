"""
Fetches MFL transactions for a range of seasons and writes one CSV per season.

Defaults:
- League ID: 60206
- TRANS_TYPE: * (all transactions)
- Seasons: 2018–2025 inclusive

Output: data/transactions/transactions_{season}.csv
"""

from __future__ import annotations

import os
import json
import time
from datetime import datetime, timezone
from typing import Any, Dict, List, Iterable

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


def _parse_id_list(s: Any) -> List[str]:
    """Parse strings like "|17171,17222," or "15857," into ["17171", "17222"]."""
    if s is None:
        return []
    if not isinstance(s, str):
        s = str(s)
    s = s.strip()
    if s.startswith('|'):
        s = s[1:]
    parts = [p.strip() for p in s.split(',') if p.strip()]
    return parts


def _safe_int(x: Any) -> int | None:
    try:
        sx = str(x).strip()
        return int(sx)
    except Exception:
        return None


def fetch_transactions(
    year: int | str,
    league_id: str = "60206",
    api_key: str = "",
    trans_types: Iterable[str] | str | None = "*",
) -> Dict[str, Any]:
    """
    Call MFL transactions endpoint and return parsed JSON.
    Uses stable api host similar to other ingestors.
    """
    # Default to all transaction types when not provided
    if trans_types is None:
        trans_types = "*"

    def _normalize_trans_types(tt: Iterable[str] | str) -> str:
        """Return a comma-separated list with no spaces. Accepts list or comma string."""
        if tt is None:
            return ''
        # If a single string is provided, split on commas
        if isinstance(tt, str):
            # Allow '*' to pass through unchanged
            if tt.strip() == '*':
                return '*'
            parts = [p.strip() for p in tt.split(',') if p.strip()]
        else:
            parts = [str(p).strip() for p in tt if str(p).strip()]
        return ','.join(parts)

    url = f"https://api.myfantasyleague.com/{year}/export"
    params = {
        'TYPE': 'transactions',
        'L': league_id,
        'APIKEY': api_key,
        # Comma-separated per API without spaces
        'TRANS_TYPE': _normalize_trans_types(trans_types),
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


def normalize_transactions(data: Dict[str, Any]) -> pd.DataFrame:
    """
    Normalize transactions JSON to a flat table.

    Columns produced (when available):
    - timestamp (int)
    - ts_utc (ISO8601 string)
    - type (str)
    - franchise (str)
    - player_ids (semicolon-separated str)
    - player_count (int)
    - added_ids (semicolon-separated str)
    - added_count (int)
    - dropped_ids (semicolon-separated str)
    - dropped_count (int)
    - raw (json string of original object)
    """
    tx_root = data.get('transactions', {})
    items = _to_list(tx_root.get('transaction'))

    rows: List[Dict[str, Any]] = []
    for obj in items:
        if not isinstance(obj, dict):
            continue

        typ = obj.get('type')
        ts = _safe_int(obj.get('timestamp'))
        ts_iso = (
            datetime.fromtimestamp(ts, tz=timezone.utc).isoformat() if ts is not None else None
        )
        franchise = obj.get('franchise')

        # Parse possible id lists in multiple shapes
        raw_tx = obj.get('transaction')
        player_ids = _parse_id_list(raw_tx)

        added_ids = _parse_id_list(obj.get('added')) if 'added' in obj else []
        dropped_ids = _parse_id_list(obj.get('dropped')) if 'dropped' in obj else []

        row: Dict[str, Any] = {
            'timestamp': ts,
            'ts_utc': ts_iso,
            'type': typ,
            'franchise': franchise,
            'player_ids': ';'.join(player_ids) if player_ids else None,
            'player_count': len(player_ids) if player_ids else 0,
            'added_ids': ';'.join(added_ids) if added_ids else None,
            'added_count': len(added_ids) if added_ids else 0,
            'dropped_ids': ';'.join(dropped_ids) if dropped_ids else None,
            'dropped_count': len(dropped_ids) if dropped_ids else 0,
            'raw': json.dumps(obj, separators=(",", ":"), ensure_ascii=False),
        }

        # Keep some potentially useful optional fields if present
        for k in ['amount', 'salary', 'notes', 'bid', 'franchise2', 'status']:
            if k in obj and k not in row:
                row[k] = obj.get(k)

        rows.append(row)

    if not rows:
        return pd.DataFrame()

    df = pd.DataFrame(rows)
    # Standardize dtypes for known columns
    if 'type' in df.columns:
        df['type'] = df['type'].astype(str)
    if 'franchise' in df.columns:
        df['franchise'] = df['franchise'].astype(str)
    return df


def save_transactions_year_csv(df: pd.DataFrame, season: int | str) -> str:
    out_dir = os.path.join(_repo_root(), 'data', 'transactions')
    _ensure_dir(out_dir)

    df = df.copy()
    df['season'] = int(season)
    cols = ['season'] + [c for c in df.columns if c != 'season']
    df = df[cols]

    out_path = os.path.join(out_dir, f'transactions_{int(season)}.csv')
    df.to_csv(out_path, index=False)
    return out_path


def process_season(year: int | str, league_id: str = '60206', api_key: str = '', trans_types: Iterable[str] | None = None) -> pd.DataFrame:
    print(f"Processing transactions for season {year}...")
    data = fetch_transactions(year, league_id, api_key, trans_types)
    df = normalize_transactions(data)
    print(f"Fetched {len(df)} transaction rows for {year}")
    out_path = save_transactions_year_csv(df, year)
    print(f"Saved CSV: {out_path}")
    return df


def process_multiple_years(years: Iterable[int], league_id: str = '60206', api_key: str = '', trans_types: Iterable[str] | None = None) -> None:
    years_list = list(years)
    for idx, y in enumerate(years_list):
        try:
            process_season(y, league_id, api_key, trans_types)
        except Exception as e:
            print(f"Error processing year {y}: {e}")
        # Sleep 3 seconds between years (not after the last one)
        if idx < len(years_list) - 1:
            time.sleep(3)


if __name__ == '__main__':
    # Default range to mirror other scripts (2018–2025 inclusive)
    years_to_process = range(2018, 2026)
    process_multiple_years(years_to_process)
