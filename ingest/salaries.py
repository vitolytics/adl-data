"""
Fetches MFL salaries for a range of seasons and writes CSVs per season.

Rules:
- Historical years: data/salaries/salaries_{YEAR}_seasonEnd.csv
- Current year (from .ENV current_season):
    - Timestamped snapshot: data/salaries/salaries_{YEAR}_asof_{mm dd yyyy hh mm}.csv
    - Stable snapshot (overwritten every run): data/salaries/salaries_{YEAR}.csv

Environment (.ENV in repo root):
- mfl_api_key
- mfl_league_id
- current_season

API host: https://api.myfantasyleague.com/{year}/export?TYPE=salaries&L=...&JSON=1
"""

from __future__ import annotations

import os
import time
from datetime import datetime
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


def fetch_salaries(year: int | str, league_id: str, api_key: str = "") -> Dict[str, Any]:
    """
    Call MFL salaries endpoint and return parsed JSON.
    """
    # Use league server host per provided example (www46)
    url = f"https://www46.myfantasyleague.com/{year}/export"
    params = {
        'TYPE': 'salaries',
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


def _coalesce_attribute_stream(items: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Coalesce sequences of single-key dicts into per-player records.
    Heuristic: start a new record when a lone 'id' appears and we already have data.
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


def normalize_salaries(data: Dict[str, Any]) -> pd.DataFrame:
    """Normalize salaries JSON to a flat DataFrame, one row per player."""
    sal_root = data.get('salaries', {})
    units = _to_list(sal_root.get('leagueUnit'))
    all_rows: List[Dict[str, Any]] = []

    for unit in units:
        unit_name = unit.get('unit') if isinstance(unit, dict) else None
        players = []
        if isinstance(unit, dict):
            players = unit.get('player')
        players = _to_list(players)

        # Detect stream of single-key dicts
        is_stream = players and all(isinstance(p, dict) and len(p.keys()) <= 2 for p in players)

        if is_stream:
            coalesced = _coalesce_attribute_stream(players)
            for p in coalesced:
                row = dict(p)
                if unit_name is not None:
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

    df = pd.DataFrame(all_rows)

    # Convert conference like "CONFERENCE00" -> 0/1
    if 'conference' in df.columns:
        def _conf_to_int(v: Any):
            if v is None:
                return None
            s = str(v).strip().upper()
            if s.startswith('CONFERENCE'):
                s = s[len('CONFERENCE'):]
            s = ''.join(ch for ch in s if ch.isdigit())
            if s == '':
                return None
            try:
                return int(s)
            except Exception:
                return None
        df['conference'] = df['conference'].map(_conf_to_int)

    # Ensure core fields are strings if present
    for col in [c for c in ['id', 'salary', 'contractYear', 'contractInfo'] if c in df.columns]:
        df[col] = df[col].astype(str)

    # Remove any lingering 'unit'
    if 'unit' in df.columns:
        df = df.drop(columns=['unit'])

    return df


def _rebuild_combined_salaries(out_dir: str) -> str | None:
    """Rebuild data/salaries/salaries_all.csv.

    - For seasons before the current season: include exactly one file per season
      in this preference order: stable (salaries_{YEAR}.csv) > seasonEnd > latest as-of.
    - For the current season: include all as-of snapshots and de-duplicate rows by
      (season, id, conference, contractYear, salary) after sorting by snapshot time,
      so we keep only the first time each unique combination appears.
    """
    import glob
    import re
    from datetime import datetime as _dt

    # Determine current season from .ENV
    env = _load_env(os.path.join(_repo_root(), '.ENV'))
    try:
        current_season = int((env.get('current_season') or env.get('CURRENT_SEASON') or '').strip())
    except Exception:
        current_season = None

    # Discover files and group by season
    files = sorted(glob.glob(os.path.join(out_dir, 'salaries_*.csv')))
    by_year: Dict[int, Dict[str, Any]] = {}

    for fp in files:
        base = os.path.basename(fp)
        if base == 'salaries_all.csv':
            continue

        m_stable = re.match(r"^salaries_(\d{4})\.csv$", base)
        m_season_end = re.match(r"^salaries_(\d{4})_seasonEnd\.csv$", base)
        m_asof = re.match(r"^salaries_(\d{4})_asof_(.+)\.csv$", base)

        try:
            if m_stable:
                year = int(m_stable.group(1))
                by_year.setdefault(year, {})['stable'] = fp
            elif m_season_end:
                year = int(m_season_end.group(1))
                by_year.setdefault(year, {})['season_end'] = fp
            elif m_asof:
                year = int(m_asof.group(1))
                ts_str = m_asof.group(2)
                # Expect format: '%m %d %Y %H %M'
                try:
                    ts = _dt.strptime(ts_str, '%m %d %Y %H %M')
                except Exception:
                    ts = None
                entry = (ts, fp, ts_str)
                bucket = by_year.setdefault(year, {}).setdefault('asofs', [])
                bucket.append(entry)
            else:
                # Fallback: try to extract year and drop into as-of bucket with mtime ordering
                m_year = re.search(r"(19|20)\d{2}", base)
                if not m_year:
                    continue
                year = int(m_year.group(0))
                try:
                    ts = _dt.fromtimestamp(os.path.getmtime(fp))
                except Exception:
                    ts = None
                by_year.setdefault(year, {}).setdefault('asofs', []).append((ts, fp, None))
        except Exception as e:
            print(f"Warning: could not categorize {fp}: {e}")

    frames: List[pd.DataFrame] = []

    for year in sorted(by_year.keys()):
        info = by_year[year]
        if current_season is not None and year == current_season:
            # Include all as-of snapshots for current season
            asofs = info.get('asofs', [])
            # Sort by timestamp ascending
            asofs_sorted = sorted(asofs, key=lambda t: (t[0] is None, t[0]))
            for ts, fp, ts_str in asofs_sorted:
                base = os.path.basename(fp)
                try:
                    df = pd.read_csv(fp)
                    # Ensure season column exists; infer from filename if missing
                    if 'season' not in df.columns:
                        df['season'] = year
                    # Add as-of timestamp column (string) for current season rows
                    # Prefer ISO-ish format for readability if parsable
                    asof_text = ts.strftime('%Y-%m-%d %H:%M') if ts else (ts_str or '')
                    df['asof'] = asof_text
                    # Reorder to have season first
                    cols = ['season'] + [c for c in df.columns if c != 'season']
                    df = df[cols]
                    frames.append(df)
                except Exception as e:
                    print(f"Warning: could not read {fp}: {e}")
        else:
            # Choose one file by preference for non-current seasons
            chosen_fp = None
            if 'stable' in info:
                chosen_fp = info['stable']
            elif 'season_end' in info:
                chosen_fp = info['season_end']
            elif 'asofs' in info and info['asofs']:
                asofs_sorted = sorted(info['asofs'], key=lambda t: (t[0] is None, t[0]))
                chosen_fp = asofs_sorted[-1][1]

            if chosen_fp:
                base = os.path.basename(chosen_fp)
                try:
                    df = pd.read_csv(chosen_fp)
                    if 'season' not in df.columns:
                        # Infer from filename parts
                        parts = base.split('_')
                        if len(parts) >= 2 and parts[1].isdigit() and len(parts[1]) == 4:
                            df['season'] = int(parts[1])
                        else:
                            m = re.search(r"(19|20)\d{2}", base)
                            df['season'] = int(m.group(0)) if m else None
                    # Reorder to have season first
                    cols = ['season'] + [c for c in df.columns if c != 'season']
                    df = df[cols]
                    frames.append(df)
                except Exception as e:
                    print(f"Warning: could not read {chosen_fp}: {e}")

    if not frames:
        return None

    combined = pd.concat(frames, ignore_index=True, sort=False)

    # For current season, de-duplicate rows across snapshots on key fields
    if current_season is not None and 'season' in combined.columns and 'id' in combined.columns:
        key_cols = ['season', 'id']
        if 'conference' in combined.columns:
            key_cols.append('conference')
        if 'contractYear' in combined.columns:
            key_cols.append('contractYear')
        if 'salary' in combined.columns:
            key_cols.append('salary')
        # Sort so earlier snapshots come first
        if 'asof' in combined.columns:
            # Attempt to sort by asof textual column if present
            combined = combined.sort_values(by=[c for c in ['season', 'asof'] if c in combined.columns])
        # Drop duplicates keeping the first occurrence
        combined = combined.drop_duplicates(subset=key_cols, keep='first', ignore_index=True)

    out_path = os.path.join(out_dir, 'salaries_all.csv')
    combined.to_csv(out_path, index=False)
    print(f"Updated combined file: {out_path} ({len(combined)} rows)")
    return out_path


def save_salaries_csv(df: pd.DataFrame, year: int, is_current_year: bool, when: datetime | None = None) -> str:
    out_dir = os.path.join(_repo_root(), 'data', 'salaries')
    _ensure_dir(out_dir)

    # Ensure 'season' column present and first
    df = df.copy()
    df['season'] = int(year)
    cols = ['season'] + [c for c in df.columns if c != 'season']
    df = df[cols]

    if is_current_year:
        when = when or datetime.now()
        ts = when.strftime('%m %d %Y %H %M')
        # 1) Timestamped snapshot
        asof_filename = f"salaries_{year}_asof_{ts}.csv"
        asof_path = os.path.join(out_dir, asof_filename)
        df.to_csv(asof_path, index=False, mode='w')
        print(f"Saved current-year as-of snapshot: {asof_path}")

        # 2) Stable latest snapshot for the year (overwrites each run)
        stable_filename = f"salaries_{year}.csv"
        stable_path = os.path.join(out_dir, stable_filename)
        df.to_csv(stable_path, index=False, mode='w')
        print(f"Saved current-year stable snapshot: {stable_path}")

        # Update combined after saving
        _rebuild_combined_salaries(out_dir)
        return asof_path
    else:
        filename = f"salaries_{year}_seasonEnd.csv"
        out_path = os.path.join(out_dir, filename)
        df.to_csv(out_path, index=False, mode='w')
        # Update combined after saving
        _rebuild_combined_salaries(out_dir)
        return out_path


def process_season(year: int, league_id: str, api_key: str, current_year: int) -> pd.DataFrame:
    print(f"Processing salaries for season {year}...")
    data = fetch_salaries(year, league_id, api_key)
    df = normalize_salaries(data)
    print(f"Fetched {len(df)} salary rows for {year}")
    out_path = save_salaries_csv(df, year, is_current_year=(year == current_year))
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
