"""
Fetches MFL player scores for the current season.
This is designed to run on a schedule through each NFL week to keep the current season data fresh.

Rules:
- Current season and week are detected from MFL (see season.py)
- By default only the previous and current week are re-fetched: that covers games played in the
  current week and stat corrections to the week that just finished, whichever week MFL reports as current
- --all-weeks re-fetches weeks 1..current week, to backfill missed runs or late stat corrections
- Updates weekly files: data/playerScores/playerScores_{YEAR}_w{WEEK}.csv
- Rewrites yearly file from every saved weekly file: data/playerScores/playerScores_{YEAR}.csv
  (only if every requested week fetched successfully)
- 5-second pause between requests

Environment (.ENV in repo root):
- mfl_api_key
- mfl_league_id

API host: https://api.myfantasyleague.com/{year}/export?TYPE=playerScores&L=...&W=...&JSON=1
"""

from __future__ import annotations

import argparse
import os

from playerScores import _load_env, _repo_root, default_weeks_for_year, process_year
from season import detect_current_season, detect_current_week


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument('--all-weeks', action='store_true', help='Re-fetch weeks 1..current week instead of the previous and current week')
    args = parser.parse_args()

    env = _load_env(os.path.join(_repo_root(), '.ENV'))
    league_id = env.get('mfl_league_id') or env.get('MFL_LEAGUE_ID') or '60206'
    api_key = env.get('mfl_api_key') or env.get('MFL_API_KEY') or ''

    current_season = detect_current_season(league_id, api_key)
    current_week = detect_current_week(current_season)
    weeks = default_weeks_for_year(current_season, current_season, current_week)
    if not args.all_weeks:
        weeks = weeks[-2:]

    print(f"\nFetching player scores for season {current_season}, weeks {weeks[0]}-{weeks[-1]}...")
    process_year(current_season, weeks, league_id, api_key)
    print("Successfully updated player scores for the current season!")
