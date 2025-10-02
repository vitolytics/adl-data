# Player Performance Consistency Metrics

## Overview

The performance consistency metrics system calculates week-to-week scoring stability for fantasy football players. In head-to-head leagues, **consistency matters** - a player who scores 12 points every week may be more valuable than one who alternates between 20 and 5 points.

## Quick Start

```bash
python ingest/performance.py
```

Output files are saved to `data/performance/`:
- `consistency_YYYY.csv` - Per-season metrics
- `consistency_all.csv` - All seasons combined

## Metrics Calculated

### 1. Basic Statistical Metrics

| Metric | Description | Interpretation |
|--------|-------------|----------------|
| **games_played** | Number of games played in the season | Minimum 8 games for historical seasons, 4 for current season |
| **mean_score** | Average fantasy points per game | Higher is better |
| **median_score** | Middle value of all scores | Less affected by outliers than mean |
| **std_dev** | Standard deviation of scores | **Lower is better** - indicates more consistency |
| **coefficient_variation** | CV = std_dev / mean | Normalized consistency measure; lower is better |
| **floor_score** | 25th percentile score | Your "safe" expectation |
| **ceiling_score** | 75th percentile score | Typical high-end performance |
| **min_score** | Lowest score in period | Worst-case scenario |
| **max_score** | Highest score in period | Best performance |

### 2. Position-Relative Metrics

These metrics show how a player compares to others at their position:

| Metric | Description | Interpretation |
|--------|-------------|----------------|
| **std_dev_vs_position** | Z-score of std_dev within position | **Negative is better** - more consistent than peers |
| **cv_vs_position** | Z-score of CV within position | **Negative is better** - more stable scoring |

**Example**: A std_dev_vs_position of -1.5 means the player is 1.5 standard deviations MORE consistent than the average player at their position.

### 3. Consistency Scores

The consistency score formula: **`mean_score / (1 + std_dev)`**

This rewards both high scoring AND low variance. Higher is always better.

| Metric | Description | Use Case |
|--------|-------------|----------|
| **consistency_score_std** | Current season consistency | Season-to-date performance |
| **consistency_score_trailing_10** | Last 10 games (across seasons) | Recent form |
| **consistency_score_trailing_5** | Last 5 games (across seasons) | Very recent trends |
| **consistency_score_last_season** | Full previous season | Year-over-year comparison |
| **consistency_score_2seasons_ago** | Two seasons back | Long-term stability |

## Understanding the Data

### Game Thresholds

- **Historical Seasons (completed)**: Minimum 8 games required
  - Ensures meaningful sample size for full seasons
  
- **Current Season**: Minimum 4 games required
  - Allows analysis early in the season

### Position Filtering

- Team defenses (position starts with "TM") are excluded
- Only players with valid position data are included
- Positions: QB, RB, WR, TE, K/PK, DEF, IDP positions, etc.

## Usage Examples

### Example 1: Find Most Consistent RBs in 2024

```python
import polars as pl

df = pl.read_csv('data/performance/consistency_2024.csv')

consistent_rbs = (
    df.filter(pl.col('position') == 'RB')
    .sort('consistency_score_std', descending=True)
    .head(10)
    .select(['player_name', 'mean_score', 'std_dev', 'consistency_score_std'])
)

print(consistent_rbs)
```

### Example 2: Identify Boom/Bust Players

```python
import polars as pl

df = pl.read_csv('data/performance/consistency_2024.csv')

# High scoring but high variance (boom/bust)
boom_bust = (
    df.filter(
        (pl.col('mean_score') > 15) &  # Good average
        (pl.col('std_dev') > 10)  # High variance
    )
    .sort('std_dev', descending=True)
)

print(boom_bust[['player_name', 'position', 'mean_score', 'std_dev']])
```

### Example 3: Compare Position Consistency

```python
import polars as pl

df = pl.read_csv('data/performance/consistency_2024.csv')

position_consistency = (
    df.group_by('position')
    .agg([
        pl.col('std_dev').mean().alias('avg_std_dev'),
        pl.col('coefficient_variation').mean().alias('avg_cv'),
        pl.count().alias('player_count')
    ])
    .filter(pl.col('player_count') >= 10)  # At least 10 players
    .sort('avg_std_dev')
)

print(position_consistency)
```

### Example 4: Track Player Trends

```python
import polars as pl

df = pl.read_csv('data/performance/consistency_all.csv')

# Track a player across seasons
player_history = (
    df.filter(pl.col('player_name').str.contains('Mahomes'))
    .select([
        'season', 'games_played', 'mean_score', 'std_dev',
        'consistency_score_std', 'std_dev_vs_position'
    ])
    .sort('season')
)

print(player_history)
```

## Use Cases for Fantasy Football

### Draft Strategy

1. **Value Consistency**: Use `consistency_score_std` to identify reliable weekly starters
2. **Floor Analysis**: Check `floor_score` to understand worst-case scenarios
3. **Position Comparison**: Use `std_dev_vs_position` to find exceptionally stable players

### Trade Analysis

1. **Recent Form**: Check `consistency_score_trailing_5` and `consistency_score_trailing_10`
2. **Trend Analysis**: Compare current season vs `consistency_score_last_season`
3. **Boom/Bust Profile**: Evaluate `std_dev` and `coefficient_variation`

### Lineup Decisions

1. **Safe Floor**: Sort by `floor_score` when you need guaranteed points
2. **High Ceiling**: Sort by `ceiling_score` when you need a home run
3. **Consistency**: Use `consistency_score_std` for predictable performance

## Technical Details

### Performance

- Uses **Polars** for fast data processing
- Typical runtime: ~5-10 seconds for 8 seasons of data
- Memory efficient: processes ~150K records with minimal overhead

### Data Flow

```
playerScores (yearly CSVs)
    ↓
players_all.csv (position metadata)
    ↓
ingest/performance.py (calculations)
    ↓
data/performance/consistency_*.csv (output)
```

### Calculation Notes

1. **Null Handling**: 
   - Null scores and IDs are filtered out
   - Missing historical seasons return null for those metrics

2. **Trailing Metrics**:
   - Sorted by season/week descending
   - Cross-season boundaries (e.g., includes end of 2023 + start of 2024)

3. **Position Stats**:
   - Calculated per season
   - Requires minimum 4 games played for inclusion in position averages

## Automation

### GitHub Actions (Future Enhancement)

You can set up automated weekly updates similar to `playerScores_weekly.yml`:

```yaml
name: Weekly Performance Metrics
on:
  schedule:
    - cron: '0 8 * * 2'  # Tuesday 8 AM UTC
  workflow_dispatch:

jobs:
  calculate-metrics:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v3
      - name: Set up Python
        uses: actions/setup-python@v4
        with:
          python-version: '3.11'
      - name: Install dependencies
        run: pip install polars numpy
      - name: Calculate performance metrics
        run: python ingest/performance.py
      - name: Commit results
        run: |
          git config user.name "GitHub Actions"
          git config user.email "actions@github.com"
          git add data/performance/
          git commit -m "Update performance metrics" || exit 0
          git push
```

## Troubleshooting

### No data output

- **Check**: Run `python ingest/players.py` first to ensure player metadata exists
- **Check**: Verify `data/playerScores/playerScores_YYYY.csv` files exist

### Memory issues

- The script uses Polars which is memory efficient
- If issues persist, process fewer seasons by filtering the yearly files

### Inconsistent results

- Ensure `.ENV` file has correct `current_season` value
- Re-run `ingest/playerScores.py` to refresh score data

## Column Reference

Full list of columns in output files:

```
season                          - NFL season year
player_id                       - Unique player identifier
player_name                     - Player name
position                        - Player position
games_played                    - Games in this analysis period
mean_score                      - Average points per game
median_score                    - Median points per game
min_score                       - Minimum score
max_score                       - Maximum score
std_dev                         - Standard deviation
coefficient_variation           - CV ratio
floor_score                     - 25th percentile
ceiling_score                   - 75th percentile
std_dev_vs_position             - Position-relative std dev z-score
cv_vs_position                  - Position-relative CV z-score
consistency_score_std           - Season consistency score
consistency_score_trailing_10   - Last 10 games consistency
consistency_score_trailing_5    - Last 5 games consistency
consistency_score_last_season   - Previous season consistency
consistency_score_2seasons_ago  - Two seasons ago consistency
```

## Support

For issues or questions:
1. Check that all data ingestion scripts have been run
2. Verify `.ENV` configuration
3. Review error messages for specific file/data issues

## Future Enhancements

Potential additions:
- Week-over-week consistency trends
- Matchup-adjusted consistency (vs different opponents)
- Consistency tiers/rankings
- Predictive consistency modeling
- Integration with roster/salary data
