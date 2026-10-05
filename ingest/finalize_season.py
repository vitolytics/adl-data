"""
Re-fetches a completed season from MFL so its files hold the final numbers
(stat corrections, last transactions, final salaries) instead of whatever the
in-season jobs last captured.

Refreshes for the given season:
- data/transactions/transactions_{YEAR}.csv
- data/players/player_{YEAR}.csv and its rows in players_all.csv
- data/playerScores/playerScores_{YEAR}_w{1..18}.csv and playerScores_{YEAR}.csv
- data/salaries/salaries_{YEAR}_seasonEnd.csv (+ salaries_all.csv)
- data/salaryAdjustments/salaryAdjustment_{YEAR}_seasonEnd.csv
- data/leagueInfo, data/franchises, data/rosterInfo for {YEAR}

Rosters are finalized automatically by rosters.py once the next season starts.

The season must be older than the current MFL season (i.e. the league has been
renewed into the next year), otherwise the script exits without changing anything.

Usage:
    python ingest/finalize_season.py            # finalizes the season before the current one
    python ingest/finalize_season.py --season 2025
"""

from __future__ import annotations

import argparse
import os
import sys
import time

import leagues
import playerScores
import players
import players_current_season
import salaries
import salary_adjustments
import transactions
from season import detect_current_season


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument('--season', type=int, default=None, help='Completed season to finalize (default: current season - 1)')
    args = parser.parse_args()

    env = transactions._load_env(os.path.join(transactions._repo_root(), '.ENV'))
    league_id = env.get('mfl_league_id') or env.get('MFL_LEAGUE_ID') or '60206'
    api_key = env.get('mfl_api_key') or env.get('MFL_API_KEY') or ''

    current_season = detect_current_season(league_id, api_key)
    season = args.season if args.season is not None else current_season - 1
    if season >= current_season:
        print(f"Season {season} is not complete: the current MFL season is {current_season}. Nothing changed.")
        return 1

    print(f"Finalizing season {season} (current season is {current_season})")

    transactions.process_season(season, league_id, api_key)
    time.sleep(2)

    players_df = players.process_year(season, league_id, api_key)
    if players_df.empty:
        raise RuntimeError(f"MFL returned no players for {season}")
    players_current_season.update_combined_csv(players_df.assign(year=season), season)
    time.sleep(2)

    playerScores.process_year(season, playerScores.default_weeks_for_year(season, current_season, None), league_id, api_key)
    time.sleep(2)

    salaries.process_season(season, league_id, api_key, current_season)
    time.sleep(2)

    salary_adjustments.process_season(season, league_id, api_key, current_season)
    time.sleep(2)

    leagues.process_single_year(season, league_id, api_key)

    print(f"Season {season} finalized.")
    return 0


if __name__ == '__main__':
    sys.exit(main())
