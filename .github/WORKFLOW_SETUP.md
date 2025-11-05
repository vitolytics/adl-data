# GitHub Workflow Setup

## Update Players Data Workflow

The `update-players.yml` workflow automatically updates the players data for the current season three times per week.

### Schedule

- **Tuesday** at 5:00 AM EST (10:00 AM UTC)
- **Thursday** at 5:00 AM EST (10:00 AM UTC)
- **Sunday** at 10:00 AM EST (3:00 PM UTC)

**Note:** GitHub Actions cron schedules use UTC time. During Daylight Saving Time (EDT), these times will be 1 hour earlier in your local time (4:00 AM EDT and 9:00 AM EDT respectively).

### Required GitHub Configuration

To enable this workflow, you need to configure secrets and variables in your GitHub repository:

#### Secrets (Sensitive Data)

1. Go to your repository on GitHub
2. Click **Settings** → **Secrets and variables** → **Actions** → **Secrets** tab
3. Add the following repository secrets:

| Secret Name | Description | Example Value |
|-------------|-------------|---------------|
| `MFL_API_KEY` | Your MyFantasyLeague API key | `your_api_key_here` |
| `MFL_LEAGUE_ID` | Your MyFantasyLeague league ID | `60206` |

#### Variables (Non-Sensitive Configuration)

1. In the same **Secrets and variables** → **Actions** page, click the **Variables** tab
2. Click **New repository variable** and add:

| Variable Name | Description | Example Value |
|---------------|-------------|---------------|
| `CURRENT_SEASON` | The current season year | `2025` |

**Note:** Using a variable (instead of a secret) for `CURRENT_SEASON` makes it easier to update each year without modifying code.

### Manual Trigger

You can manually trigger the workflow at any time:

1. Go to **Actions** tab in your GitHub repository
2. Select **Update Players Data** workflow
3. Click **Run workflow** button
4. Choose the branch and click **Run workflow**

### What the Workflow Does

1. Checks out the repository with full git history
2. Sets up Python 3.12 environment with pip caching
3. Installs required dependencies from `requirements.txt`
4. Creates a temporary `.ENV` file from GitHub secrets/variables
5. Runs the `ingest/players_current_season.py` script (optimized to only update the current season)
6. Updates `data/players/player_YYYY.csv` and `data/players/players_all.csv`
7. Removes the temporary `.ENV` file for security
8. Rebases on latest changes to minimize conflicts
9. Commits and pushes any changes to the players CSV files
10. Uses the `github-actions[bot]` account for commits

**Note:** The workflow uses `players_current_season.py` which only fetches data for the current season, making it faster and more efficient than the full `players.py` script which processes all years from 2018 onwards.

**Concurrency:** Only one instance of this workflow can run at a time to prevent conflicts.

### Troubleshooting

- **Workflow not running:** Check that GitHub Actions is enabled for your repository
- **Authentication errors:** Verify that both secrets (`MFL_API_KEY`, `MFL_LEAGUE_ID`) and the variable (`CURRENT_SEASON`) are set correctly
- **No changes committed:** This is normal if the player data hasn't changed since the last run
- **Rate limit errors:** The script will exit gracefully if MFL rate limits are hit
- **Push conflicts:** The workflow automatically rebases on the latest changes to minimize conflicts

