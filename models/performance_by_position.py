"""
Generates position-level summary statistics for top N players per position.

This filters to "replacement level" players based on league settings to reduce noise.
Only the top N players by value_score are included for each position.

Output files:
- data/performance/position_summary_{YEAR}.csv (per season)
- data/performance/position_summary_all.csv (all seasons)

Configuration:
- Edit POSITION_LIMITS dict below to match your league settings

Usage:
    python ingest/performance_by_position.py
"""

from __future__ import annotations

import os
from datetime import datetime
from typing import Dict

import polars as pl


def _repo_root() -> str:
    return os.path.abspath(os.path.join(os.path.dirname(__file__), os.pardir))


def _ensure_dir(p: str) -> None:
    os.makedirs(p, exist_ok=True)


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
            v = v.strip()
            if '#' in v:
                v = v.split('#', 1)[0].strip()
            if (len(v) >= 2) and ((v[0] == v[-1]) and v[0] in ('"', "'")):
                v = v[1:-1]
            else:
                v = v.strip('"').strip("'")
            result[k] = v
    return result


# League Configuration - Replacement Level
# Based on 16 teams per conference (32 total teams split into 2 conferences)
# Each conference operates as a separate league for fantasy purposes
# Starting lineup: QB, RB(1-2), WR(2-4), TE(1-2), PK, PN, DT(2-3), DE(2-3), LB(1-3), CB(2-4), S(2-3)

POSITION_LIMITS = {
    # Offensive Positions (16 teams per conference)
    'QB': 16,    # 1 starter per team
    'RB': 32,    # 2 starters per team (1-2 range, using max)
    'WR': 64,    # 4 starters per team (2-4 range, using max)
    'TE': 32,    # 2 starters per team (1-2 range, using max)
    'PK': 16,    # 1 kicker per team
    'K': 16,     # 1 kicker per team (alternate name)
    'PN': 16,    # 1 punter per team
    
    # IDP Positions (16 teams per conference)
    'DT': 48,    # 3 starters per team (2-3 range, using max)
    'DE': 48,    # 3 starters per team (2-3 range, using max)
    'LB': 48,    # 3 starters per team (1-3 range, using max)
    'CB': 64,    # 4 starters per team (2-4 range, using max)
    'S': 48,     # 3 starters per team (2-3 range, using max)
    
    # Combined positions if used
    'DL': 48,    # If DT/DE are grouped
    'DB': 64,    # If CB/S are grouped
}


def filter_to_replacement_level(df: pl.DataFrame) -> pl.DataFrame:
    """
    Filter to top N players per position per season based on replacement level.
    Uses mean_score (total points/volume) as the ranking metric.
    This identifies the actual top performers, then shows their consistency metrics.
    """
    filtered_dfs = []
    
    # Group by season AND position to get top N for each position in each season
    for (season, position), group_df in df.group_by(['season', 'position']):
        # Get the limit for this position (default to 32 if not specified)
        limit = POSITION_LIMITS.get(position, 32)
        
        # Take top N by mean_score (volume) - the highest scorers for this position in this season
        top_n = group_df.sort('mean_score', descending=True).head(limit)
        filtered_dfs.append(top_n)
    
    return pl.concat(filtered_dfs)


def calculate_position_summaries(df: pl.DataFrame) -> pl.DataFrame:
    """
    Calculate aggregate statistics by position.
    """
    position_stats = (
        df.group_by(['season', 'position'])
        .agg([
            pl.count().alias('num_players'),
            pl.col('mean_score').mean().alias('avg_mean_score'),
            pl.col('mean_score').median().alias('median_mean_score'),
            pl.col('std_dev').mean().alias('avg_std_dev'),
            pl.col('std_dev').median().alias('median_std_dev'),
            pl.col('coefficient_variation').mean().alias('avg_cv'),
            pl.col('coefficient_variation').median().alias('median_cv'),
            pl.col('floor_score').mean().alias('avg_floor_score'),
            pl.col('floor_score').median().alias('median_floor_score'),
            pl.col('value_score').mean().alias('avg_value_score'),
            pl.col('value_score').median().alias('median_value_score'),
            pl.col('consistency_score_std').mean().alias('avg_consistency_score'),
            pl.col('consistency_score_std').median().alias('median_consistency_score'),
            # Top player in position
            pl.col('player_name').first().alias('top_player'),
            pl.col('value_score').max().alias('top_value_score'),
        ])
        .sort(['season', 'position'])
    )
    
    return position_stats


def save_filtered_player_data(df: pl.DataFrame, output_suffix: str = 'replacement_level'):
    """
    Save filtered player-level data (top N per position).
    """
    out_dir = os.path.join(_repo_root(), 'data', 'performance')
    _ensure_dir(out_dir)
    
    # Save yearly files
    for (season,), season_df in df.group_by('season'):
        out_path = os.path.join(out_dir, f'consistency_{output_suffix}_{season}.csv')
        season_df.write_csv(out_path)
        print(f"[OK] Saved {out_path}")
    
    # Save combined file
    all_path = os.path.join(out_dir, f'consistency_{output_suffix}_all.csv')
    df.write_csv(all_path)
    print(f"[OK] Saved {all_path}")


def save_position_summaries(df: pl.DataFrame):
    """
    Save position-level summary statistics.
    """
    out_dir = os.path.join(_repo_root(), 'data', 'performance')
    _ensure_dir(out_dir)
    
    # Save yearly files
    for (season,), season_df in df.group_by('season'):
        out_path = os.path.join(out_dir, f'position_summary_{season}.csv')
        season_df.write_csv(out_path)
        print(f"[OK] Saved {out_path}")
    
    # Save combined file
    all_path = os.path.join(out_dir, 'position_summary_all.csv')
    df.write_csv(all_path)
    print(f"[OK] Saved {all_path}")


def main():
    """Main execution function."""
    print("=" * 60)
    print("Position-Level Performance Summary Generator")
    print("=" * 60)
    
    # Load environment
    env_path = os.path.join(_repo_root(), '.ENV')
    env = _load_env(env_path)
    current_season = int(env.get('current_season', datetime.now().year))
    
    # Load full consistency data
    print("\nLoading consistency data...")
    consistency_path = os.path.join(_repo_root(), 'data', 'performance', 'consistency_all.csv')
    
    if not os.path.exists(consistency_path):
        print("[ERROR] Consistency data not found!")
        print("Please run 'python ingest/performance.py' first.")
        return
    
    df = pl.read_csv(consistency_path)
    print(f"  Loaded {len(df)} player-seasons")
    
    # Show replacement level configuration
    print("\nReplacement level configuration:")
    for pos, limit in sorted(POSITION_LIMITS.items()):
        count = len(df.filter(pl.col('position') == pos))
        if count > 0:
            print(f"  {pos:6s}: Top {limit:3d} players (total available: {count})")
    
    # Filter to replacement level
    print("\nFiltering to replacement level players...")
    filtered_df = filter_to_replacement_level(df)
    print(f"  Filtered from {len(df)} to {len(filtered_df)} player-seasons")
    
    # Calculate position summaries
    print("\nCalculating position-level statistics...")
    position_summaries = calculate_position_summaries(filtered_df)
    print(f"  Generated {len(position_summaries)} position-season combinations")
    
    # Save results
    print("\nSaving filtered player data...")
    save_filtered_player_data(filtered_df, 'replacement_level')
    
    print("\nSaving position summaries...")
    save_position_summaries(position_summaries)
    
    print("\n" + "=" * 60)
    print("[OK] Position analysis complete!")
    print("=" * 60)
    
    # Show sample output for latest season
    print(f"\nPosition summary for {current_season} season:")
    latest_summary = position_summaries.filter(pl.col('season') == current_season)
    
    if len(latest_summary) > 0:
        print(latest_summary.select([
            'position', 'num_players', 'avg_mean_score', 'avg_std_dev', 
            'avg_cv', 'avg_value_score', 'top_player'
        ]))
    else:
        # Show most recent season available
        max_season = position_summaries['season'].max()
        print(f"\n(Current season not available, showing {max_season}):")
        latest_summary = position_summaries.filter(pl.col('season') == max_season)
        print(latest_summary.select([
            'position', 'num_players', 'avg_mean_score', 'avg_std_dev', 
            'avg_cv', 'avg_value_score', 'top_player'
        ]))
    
    print("\n" + "=" * 60)
    print("Output files created:")
    print("  1. consistency_replacement_level_YYYY.csv - Top N players per position")
    print("  2. position_summary_YYYY.csv - Position-level aggregate stats")
    print("=" * 60)


if __name__ == '__main__':
    main()
