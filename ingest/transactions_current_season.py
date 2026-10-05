"""
Fetches MFL transactions for the current season only.

Reads config from .ENV (mfl_api_key, mfl_league_id); the season is detected from MFL (see season.py).
Output: data/transactions/transactions_{season}.csv
"""

from __future__ import annotations

import os

from season import detect_current_season
from transactions import (
    process_season,
    _load_env,
    _repo_root,
)


def main():
    root = _repo_root()
    env = _load_env(os.path.join(root, '.ENV'))
    league_id = env.get('mfl_league_id') or env.get('MFL_LEAGUE_ID') or '60206'
    api_key = env.get('mfl_api_key') or env.get('MFL_API_KEY') or ''
    year = detect_current_season(league_id, api_key)

    print(f"Fetching transactions for season {year}, league {league_id}...")
    process_season(year, league_id, api_key)


if __name__ == '__main__':
    main()
