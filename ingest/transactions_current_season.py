"""
Fetches MFL transactions for the current season only.

Reads config from .ENV (mfl_api_key, mfl_league_id, current_season).
Output: data/transactions/transactions_{season}.csv
"""

from __future__ import annotations

import os
from typing import Dict

from transactions import (
    process_season,
    _repo_root,
)


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


def main():
    root = _repo_root()
    env = _load_env(os.path.join(root, '.ENV'))
    year = env.get('current_season') or env.get('CURRENT_SEASON') or '2025'
    league_id = env.get('mfl_league_id') or env.get('MFL_LEAGUE_ID') or '60206'
    api_key = env.get('mfl_api_key') or env.get('MFL_API_KEY') or ''

    print(f"Fetching transactions for season {year}, league {league_id}...")
    process_season(year, league_id, api_key)


if __name__ == '__main__':
    main()

