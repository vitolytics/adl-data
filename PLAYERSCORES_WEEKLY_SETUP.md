# Player Scores Weekly Update - Setup Guide

## What Was Created

### 1. New Script: `ingest/playerScores_current.py`

This script is a focused version of `playerScores.py` that:

- Only fetches data for the current season and current week
- **Automatically detects the current NFL week** using ESPN's public API (`https://site.api.espn.com/apis/site/v2/sports/football/nfl/scoreboard`)
- Updates both the weekly CSV file (e.g., `playerScores_2025_w14.csv`)
- Updates the yearly CSV file (e.g., `playerScores_2025.csv`) by replacing any existing data for that week
- Uses the same logic and API calls as the original script
- Designed to run quickly and efficiently for weekly updates
- No manual week number updates required!

### 2. GitHub Actions Workflow: `.github/workflows/playerscores-weekly.yml`

This workflow:

- Runs automatically every Tuesday at 6:00 AM UTC
- Can also be triggered manually via workflow_dispatch
- **Automatically detects the current week** using ESPN's public API
- Fetches player scores for just the current week of the current season
- Commits and pushes any changes back to the repository
- Uses GitHub secrets and variables for configuration

## Setup Instructions

### 1. GitHub Secrets (if not already set)

Go to your repository Settings → Secrets and variables → Actions → Secrets

Add these secrets:

- `MFL_API_KEY`: Your MyFantasyLeague API key
- `MFL_LEAGUE_ID`: Your league ID (e.g., '60206')

### 2. GitHub Variables

Go to your repository Settings → Secrets and variables → Actions → Variables

Add this variable:

- `CURRENT_SEASON`: The current season year (e.g., '2025')

**Note**: You no longer need to manually update a `CURRENT_WEEK` variable! The script automatically detects the current NFL week using the NFL API.

### 3. Testing

#### Test Locally

```bash
# Make sure your .ENV file has:
# mfl_api_key='YOUR_KEY'
# mfl_league_id='60206'
# current_season='2025'
# Note: current_week is no longer needed!

python ingest/playerScores_current.py
```

#### Test GitHub Action

1. Go to Actions tab in your GitHub repository
2. Click "Weekly Player Scores Ingest" workflow
3. Click "Run workflow" button
4. Select the main branch and run

### 4. Scheduled Execution

The workflow will run automatically every Tuesday at 6:00 AM UTC.

To change the schedule, edit the cron expression in `.github/workflows/playerscores-weekly.yml`:
```yaml
- cron: '0 6 * * 2'  # Tuesday at 6:00 AM UTC
```

Cron format: `minute hour day-of-month month day-of-week`
- Day of week: 0 = Sunday, 1 = Monday, 2 = Tuesday, etc.

Examples:
- `0 6 * * 3` = Wednesday at 6:00 AM UTC
- `30 14 * * 2` = Tuesday at 2:30 PM UTC

## How It Works

1. **Weekly Update Flow**:
   - GitHub Action triggers on Tuesday morning
   - Script calls ESPN's public API to detect the current week number automatically
   - Fetches player scores for the current week from MFL API
   - Saves/updates the weekly CSV file
   - Updates the yearly CSV by removing old data for that week and adding new data
   - Commits changes back to the repository

2. **File Structure**:
   - Weekly: `data/playerScores/playerScores_{YEAR}_w{WEEK}.csv`
   - Yearly: `data/playerScores/playerScores_{YEAR}.csv`

3. **Week Detection**:
   - Uses ESPN's public API: `https://site.api.espn.com/apis/site/v2/sports/football/nfl/scoreboard`
   - Automatically determines the current week based on the live NFL schedule
   - Falls back to week 1 if the API is unavailable
   - No manual updates needed!
   - No authentication required!

## Automation Tips

### ✅ Fully Automated (Current Solution)

The script now uses the NFL API to automatically detect the current week. No manual intervention needed!

## Troubleshooting

- **No data returned**: Verify that the current week has been played and data is available in MFL
- **Wrong week detected**: The ESPN API should always return the current week; check the script output to see what week was detected
- **API rate limits**: The script includes retry logic and respectful pacing
- **Workflow not running**: Check that the workflow file is on the main branch
- **Authentication errors**: Verify `MFL_API_KEY` and `MFL_LEAGUE_ID` secrets are set correctly
- **ESPN API unavailable**: Script falls back to week 1 if the ESPN API cannot be reached
