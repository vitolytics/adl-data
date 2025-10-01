"""
Quick script to backfill specific weeks for a season.
Usage: python ingest/backfill_weeks.py
"""

import os
import sys
import time

# Import functions from playerScores_current
from playerScores_current import (
    _repo_root, _load_env, fetch_player_scores, 
    normalize_player_scores, save_weekly_csv, update_yearly_csv
)

if __name__ == '__main__':
    root = _repo_root()
    env = _load_env(os.path.join(root, '.ENV'))
    
    league_id = env.get('mfl_league_id') or env.get('MFL_LEAGUE_ID') or '60206'
    api_key = env.get('mfl_api_key') or env.get('MFL_API_KEY') or ''
    
    # Configure backfill
    season = 2025
    weeks_to_backfill = [2, 3, 4]
    
    print(f"Backfilling season {season}, weeks: {weeks_to_backfill}")
    print("=" * 60)
    
    for week in weeks_to_backfill:
        try:
            print(f"\n--- Processing Week {week} ---")
            
            # Fetch data
            data = fetch_player_scores(season, week, league_id, api_key)
            df = normalize_player_scores(data, fallback_week=week)
            
            if df is None or len(df) == 0:
                print(f"  No data returned for week {week}")
                continue
            
            print(f"  Retrieved {len(df)} player score records")
            
            # Save files
            save_weekly_csv(df, season, week)
            update_yearly_csv(df, season, week)
            
            print(f"  ✓ Successfully updated week {week}")
            
            # Respectful pacing between requests
            if week != weeks_to_backfill[-1]:
                time.sleep(5)
                
        except Exception as e:
            print(f"  ✗ Error processing week {week}: {e}")
    
    print("\n" + "=" * 60)
    print("Backfill complete!")
