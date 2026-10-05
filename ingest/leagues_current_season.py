"""
Fetches the MFL league export for the current season only.

Reads config from .ENV (mfl_api_key, mfl_league_id); the season is detected from MFL (see season.py).
Outputs (per-year file for the season, and the matching _all.csv rebuilt):
- data/franchises/franchises_{season}.csv
- data/leagueInfo/leagueInfo_{season}.csv
- data/rosterInfo/rosterInfo_{season}.csv
"""

from __future__ import annotations

import os

from leagues import process_single_year
from season import detect_current_season
from transactions import _load_env, _repo_root


def main():
    env = _load_env(os.path.join(_repo_root(), '.ENV'))
    league_id = env.get('mfl_league_id') or env.get('MFL_LEAGUE_ID') or '60206'
    api_key = env.get('mfl_api_key') or env.get('MFL_API_KEY') or ''
    year = detect_current_season(league_id, api_key)

    process_single_year(year, league_id, api_key)


if __name__ == '__main__':
    main()
