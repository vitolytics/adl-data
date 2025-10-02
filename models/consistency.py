"""
Calculates player scoring consistency metrics from playerScores data.

Metrics calculated:
1. Standard Deviation (absolute)
2. Coefficient of Variation (CV = std_dev / mean)
3. Floor Score (25th percentile)
4. Position-relative Standard Deviation (z-score within position)
5. Position-relative CV (z-score within position)
6. Consistency Score - Last 10 games
7. Consistency Score - Last 5 games
8. Consistency Score - Last Season (full season)
9. Consistency Score - 2 Seasons Ago (full season)
10. Consistency Score - Season to Date (current season)
11. Consistency Index vs Average (100 = position average)
12. Consistency Index vs Replacement (100 = replacement level / 25th percentile)

Consistency Score Formula: mean_score / (1 + std_dev)
- Higher is better (rewards high scoring + low variance)

Consistency Indexes:
- Index vs Average: Normalized to 100 = position average, >100 = above average
- Index vs Replacement: Normalized to 100 = replacement level (25th percentile), >100 = above replacement

Output files:
- data/performance/consistency_{YEAR}.csv (per season)
- data/performance/consistency_all.csv (all seasons combined)

Environment (.ENV in repo root):
- current_season

Usage:
    python models/consistency.py
"""

from __future__ import annotations

import os
from datetime import datetime
from typing import Dict, List

import polars as pl
import numpy as np


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
            # Remove inline comments first, then strip surrounding quotes
            if '#' in v:
                v = v.split('#', 1)[0].strip()
            # Now remove wrapping quotes if present
            if (len(v) >= 2) and ((v[0] == v[-1]) and v[0] in ('"', "'")):
                v = v[1:-1]
            else:
                v = v.strip('"').strip("'")
            result[k] = v
    return result


def load_player_scores() -> pl.DataFrame:
    """Load all playerScores data from yearly CSV files."""
    scores_dir = os.path.join(_repo_root(), 'data', 'playerScores')
    
    # Find all yearly files (not weekly files)
    yearly_files = [f for f in os.listdir(scores_dir) 
                   if f.startswith('playerScores_') and f.endswith('.csv') 
                   and '_w' not in f]
    
    if not yearly_files:
        raise ValueError("No playerScores files found!")
    
    dfs = []
    for file in yearly_files:
        path = os.path.join(scores_dir, file)
        # Read with explicit schema to avoid type conflicts
        df = pl.read_csv(path, infer_schema_length=10000)
        dfs.append(df)
    
    # Concatenate with rechunk to align schemas
    all_scores = pl.concat(dfs, rechunk=True, how='vertical_relaxed')
    
    # Ensure proper types and clean data
    all_scores = (
        all_scores
        .filter(pl.col('id').is_not_null() & pl.col('score').is_not_null())
        .with_columns([
            pl.col('season').cast(pl.Int64),
            pl.col('week').cast(pl.Int64),
            pl.col('id').cast(pl.Float64).cast(pl.Int64).cast(pl.Utf8),
            pl.col('score').cast(pl.Float64)
        ])
    )
    
    return all_scores


def load_players() -> pl.DataFrame:
    """Load player metadata (for position info)."""
    players_path = os.path.join(_repo_root(), 'data', 'players', 'players_all.csv')
    
    if not os.path.exists(players_path):
        raise ValueError("Players file not found! Run ingest/players.py first.")
    
    players = pl.read_csv(players_path)
    
    # Get most recent position for each player (in case positions changed)
    players = (
        players
        .with_columns(pl.col('id').cast(pl.Utf8))
        .sort('year', descending=True)
        .unique(subset=['id'], keep='first')
        .select(['id', 'position', 'name'])
    )
    
    return players


def calculate_consistency_score(scores: list) -> float:
    """
    Calculate consistency score: mean / (1 + std_dev)
    Higher is better (rewards high scoring with low variance)
    """
    if len(scores) < 2:
        return None
    
    scores_arr = np.array(scores)
    mean_score = scores_arr.mean()
    std_dev = scores_arr.std()
    
    return mean_score / (1 + std_dev)


def calculate_value_score(mean_score: float, std_dev: float, cv: float) -> float:
    """
    Calculate value score that balances volume (mean) with consistency.
    
    Formula: mean_score * (1 / (1 + CV))
    
    This rewards:
    - High volume (mean_score)
    - Low variability relative to scoring (CV)
    
    Higher is better.
    """
    if mean_score is None or cv is None or cv < 0:
        return None
    
    # Weight volume by consistency factor
    # CV closer to 0 = more consistent = higher multiplier
    consistency_factor = 1 / (1 + cv)
    
    return mean_score * consistency_factor


def calculate_basic_metrics(scores: list) -> Dict[str, float]:
    """Calculate basic statistical metrics for a series of scores."""
    if len(scores) < 2:
        return {
            'games_played': len(scores),
            'mean_score': scores[0] if len(scores) > 0 else None,
            'median_score': None,
            'std_dev': None,
            'coefficient_variation': None,
            'floor_score': None,
            'ceiling_score': None,
            'min_score': None,
            'max_score': None,
        }
    
    scores_arr = np.array(scores)
    mean_score = scores_arr.mean()
    std_dev = scores_arr.std()
    
    return {
        'games_played': len(scores),
        'mean_score': mean_score,
        'median_score': float(np.median(scores_arr)),
        'std_dev': std_dev,
        'coefficient_variation': (std_dev / mean_score) if mean_score != 0 else None,
        'floor_score': float(np.percentile(scores_arr, 25)),
        'ceiling_score': float(np.percentile(scores_arr, 75)),
        'min_score': float(scores_arr.min()),
        'max_score': float(scores_arr.max()),
    }


def calculate_trailing_consistency(player_scores: pl.DataFrame, n_games: int) -> float:
    """Calculate consistency score for last N games."""
    if len(player_scores) < n_games:
        return None
    
    # Sort by season and week descending to get most recent games
    recent = player_scores.sort(['season', 'week'], descending=True).head(n_games)
    scores = recent['score'].to_list()
    
    return calculate_consistency_score(scores)


def calculate_season_consistency(all_scores: pl.DataFrame, player_id: str, season: int, 
                                current_season: int, min_games_historical: int = 8, min_games_current: int = 4) -> float:
    """Calculate consistency score for a specific season."""
    season_scores = all_scores.filter(
        (pl.col('id') == player_id) & 
        (pl.col('season') == season)
    )
    
    # Use appropriate threshold based on whether it's the current season
    min_games = min_games_current if season == current_season else min_games_historical
    
    if len(season_scores) < min_games:
        return None
    
    return calculate_consistency_score(season_scores['score'].to_list())


def add_position_relative_metrics(df: pl.DataFrame) -> pl.DataFrame:
    """
    Add position-relative z-scores for std_dev and CV.
    This shows how consistent a player is relative to others at their position.
    """
    if len(df) == 0:
        return df
    
    # Filter to players with enough games for meaningful comparison
    valid_players = df.filter(pl.col('games_played') >= 4)
    
    if len(valid_players) == 0:
        # Add empty columns if no valid players
        return df.with_columns([
            pl.lit(None).cast(pl.Float64).alias('std_dev_vs_position'),
            pl.lit(None).cast(pl.Float64).alias('cv_vs_position')
        ])
    
    # Calculate position-level statistics
    position_stats = (
        valid_players
        .group_by('position')
        .agg([
            pl.col('std_dev').mean().alias('pos_std_mean'),
            pl.col('std_dev').std().alias('pos_std_std'),
            pl.col('coefficient_variation').mean().alias('pos_cv_mean'),
            pl.col('coefficient_variation').std().alias('pos_cv_std')
        ])
    )
    
    # Join position stats back
    df = df.join(position_stats, on='position', how='left')
    
    # Calculate z-scores (negative z-score means more consistent than position average)
    df = df.with_columns([
        ((pl.col('std_dev') - pl.col('pos_std_mean')) / pl.col('pos_std_std')).alias('std_dev_vs_position'),
        ((pl.col('coefficient_variation') - pl.col('pos_cv_mean')) / pl.col('pos_cv_std')).alias('cv_vs_position')
    ])
    
    # Drop intermediate columns
    df = df.drop(['pos_std_mean', 'pos_std_std', 'pos_cv_mean', 'pos_cv_std'])
    
    return df


def add_consistency_score_indexes(df: pl.DataFrame) -> pl.DataFrame:
    """
    Add indexed consistency scores where 100 = position average and 100 = replacement level.
    
    Indexes added:
    - consistency_index_vs_avg: 100 = position average, >100 = better than average
    - consistency_index_vs_replacement: 100 = replacement level (25th percentile), >100 = above replacement
    """
    if len(df) == 0:
        return df
    
    # Filter to players with enough games and valid consistency scores
    valid_players = df.filter(
        (pl.col('games_played') >= 4) & 
        pl.col('consistency_score_std').is_not_null()
    )
    
    if len(valid_players) == 0:
        # Add empty columns if no valid players
        return df.with_columns([
            pl.lit(None).cast(pl.Float64).alias('consistency_index_vs_avg'),
            pl.lit(None).cast(pl.Float64).alias('consistency_index_vs_replacement')
        ])
    
    # Calculate position-level consistency metrics
    position_consistency = (
        valid_players
        .group_by('position')
        .agg([
            pl.col('consistency_score_std').mean().alias('pos_consistency_mean'),
            pl.col('consistency_score_std').quantile(0.25).alias('pos_consistency_replacement')
        ])
    )
    
    # Join position stats back
    df = df.join(position_consistency, on='position', how='left')
    
    # Calculate indexes:
    # Index vs Average: (player_score / position_mean) * 100
    # Index vs Replacement: (player_score / replacement_level) * 100
    df = df.with_columns([
        ((pl.col('consistency_score_std') / pl.col('pos_consistency_mean')) * 100).alias('consistency_index_vs_avg'),
        ((pl.col('consistency_score_std') / pl.col('pos_consistency_replacement')) * 100).alias('consistency_index_vs_replacement')
    ])
    
    # Drop intermediate columns
    df = df.drop(['pos_consistency_mean', 'pos_consistency_replacement'])
    
    return df


def calculate_performance_metrics(current_season: int, min_games_historical: int = 8, min_games_current: int = 4) -> pl.DataFrame:
    """
    Calculate all performance consistency metrics.
    
    Args:
        current_season: The current season year
        min_games_historical: Minimum games required for completed seasons (default: 8)
        min_games_current: Minimum games required for current season (default: 4)
    
    Returns:
        DataFrame with all calculated metrics per player per season
    """
    print("Loading player scores...")
    all_scores = load_player_scores()
    print(f"  Loaded {len(all_scores)} score records for {all_scores['id'].n_unique()} unique players")
    
    print("Loading player metadata...")
    players = load_players()
    print(f"  Loaded {len(players)} player records")
    
    # Merge position info
    print("Merging position data...")
    all_scores = all_scores.join(players.select(['id', 'position', 'name']), on='id', how='left')
    print(f"  After merge: {len(all_scores)} records")
    
    # Filter out team defenses and players without positions
    all_scores = all_scores.filter(
        pl.col('position').is_not_null() & 
        (~pl.col('position').str.starts_with('TM'))
    )
    print(f"  After filtering: {len(all_scores)} records for {all_scores['id'].n_unique()} players")
    
    print(f"Calculating metrics for {all_scores['id'].n_unique()} players...")
    print(f"Min games threshold: {min_games_historical} for historical seasons, {min_games_current} for current season")
    
    results = []
    
    # Group by player and season for season-level metrics
    for (player_id, season), group_df in all_scores.group_by(['id', 'season']):
        # Use different thresholds for current vs historical seasons
        min_games = min_games_current if season == current_season else min_games_historical
        
        if len(group_df) < min_games:
            continue
        
        # Basic metrics for this season
        scores = group_df['score'].to_list()
        metrics = calculate_basic_metrics(scores)
        
        # Get player info
        player_name = group_df['name'][0] if 'name' in group_df.columns else 'Unknown'
        position = group_df['position'][0]
        
        record = {
            'season': season,
            'player_id': player_id,
            'player_name': player_name,
            'position': position,
            **metrics
        }
        
        # Season-specific consistency scores
        # Season to Date (same as current season full stats if season == current_season)
        record['consistency_score_std'] = calculate_consistency_score(scores)
        
        # Last Season (previous year)
        record['consistency_score_last_season'] = calculate_season_consistency(
            all_scores, player_id, season - 1, current_season, min_games_historical, min_games_current
        )
        
        # 2 Seasons Ago
        record['consistency_score_2seasons_ago'] = calculate_season_consistency(
            all_scores, player_id, season - 2, current_season, min_games_historical, min_games_current
        )
        
        # Trailing periods (last N games across seasons)
        player_all_games = all_scores.filter(pl.col('id') == player_id).sort(['season', 'week'])
        
        # Filter to games up to and including this season
        player_through_season = player_all_games.filter(pl.col('season') <= season)
        
        record['consistency_score_trailing_10'] = calculate_trailing_consistency(player_through_season, 10)
        record['consistency_score_trailing_5'] = calculate_trailing_consistency(player_through_season, 5)
        
        # Calculate value score (volume + consistency balance)
        record['value_score'] = calculate_value_score(
            metrics['mean_score'], 
            metrics['std_dev'], 
            metrics['coefficient_variation']
        )
        
        results.append(record)
    
    if not results:
        print("Warning: No player-seasons met minimum games threshold!")
        return pl.DataFrame()
    
    df = pl.DataFrame(results)
    
    # Add position-relative metrics
    print("Calculating position-relative metrics...")
    
    # Group by season to calculate position-relative metrics within each season
    season_dfs = []
    for (season,), season_df in df.group_by('season'):
        season_with_relative = add_position_relative_metrics(season_df)
        season_with_indexes = add_consistency_score_indexes(season_with_relative)
        season_dfs.append(season_with_indexes)
    
    df = pl.concat(season_dfs)
    
    # Reorder columns for readability
    col_order = [
        'season', 'player_id', 'player_name', 'position', 'games_played',
        'mean_score', 'median_score', 'min_score', 'max_score',
        'std_dev', 'coefficient_variation', 'floor_score', 'ceiling_score',
        'std_dev_vs_position', 'cv_vs_position',
        'value_score',
        'consistency_score_std', 'consistency_index_vs_avg', 'consistency_index_vs_replacement',
        'consistency_score_trailing_10', 'consistency_score_trailing_5',
        'consistency_score_last_season', 'consistency_score_2seasons_ago'
    ]
    
    # Only include columns that exist
    col_order = [col for col in col_order if col in df.columns]
    df = df.select(col_order)
    
    # Sort by season and value score (best balance of volume + consistency)
    df = df.sort(['season', 'value_score'], descending=[True, True])
    
    return df


def save_performance_data(df: pl.DataFrame, current_season: int):
    """Save performance metrics to CSV files."""
    out_dir = os.path.join(_repo_root(), 'data', 'performance')
    _ensure_dir(out_dir)
    
    # Save yearly files
    for (season,), season_df in df.group_by('season'):
        out_path = os.path.join(out_dir, f'consistency_{season}.csv')
        season_df.write_csv(out_path)
        print(f"[OK] Saved {out_path}")
    
    # Save combined file
    all_path = os.path.join(out_dir, 'consistency_all.csv')
    df.write_csv(all_path)
    print(f"[OK] Saved {all_path}")


def main():
    """Main execution function."""
    print("=" * 60)
    print("Player Scoring Consistency Metrics Calculator")
    print("=" * 60)
    
    # Load environment
    env_path = os.path.join(_repo_root(), '.ENV')
    env = _load_env(env_path)
    
    current_season = int(env.get('current_season', datetime.now().year))
    print(f"Current season: {current_season}")
    
    # Calculate metrics
    # Use 8 games minimum for historical seasons, 4 for current season
    df = calculate_performance_metrics(current_season, min_games_historical=8, min_games_current=4)
    
    if len(df) == 0:
        print("\n[WARNING] No data to process. Make sure playerScores and players data exists.")
        return
    
    print(f"\nCalculated metrics for {len(df)} player-seasons")
    print(f"Seasons covered: {df['season'].min()} - {df['season'].max()}")
    print(f"Unique players: {df['player_id'].n_unique()}")
    
    # Save results
    print("\nSaving results...")
    save_performance_data(df, current_season)
    
    print("\n" + "=" * 60)
    print("[OK] Performance metrics calculation complete!")
    print("=" * 60)
    
    # Show sample output
    print("\nTop 10 players by VALUE SCORE (best balance of volume + consistency):")
    max_season = df['season'].max()
    latest = df.filter(pl.col('season') == max_season).head(10)
    print(latest.select(['player_name', 'position', 'games_played', 'mean_score', 
                        'consistency_score_std', 'consistency_index_vs_avg', 
                        'consistency_index_vs_replacement', 'value_score']))


if __name__ == '__main__':
    main()
