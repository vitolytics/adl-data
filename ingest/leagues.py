'''
Ingests MFL leagues endpoint
Outputs: leagueInfo, franchises, rosterInfo

'''


import requests
import pandas as pd
import json
import os
import glob
from typing import Union
import time

def fetch_league_data(year, league_id="60206", api_key=""):
    """
    Fetch fantasy league data for a given year
    
    Args:
        year (str/int): The season year (e.g., 2025)
        league_id (str): The league ID (default: 60206)
        api_key (str): API key if required (default: empty)
    
    Returns:
        dict: JSON response from the API
    """
    # Use stable API host that works across seasons
    url = f"https://api.myfantasyleague.com/{year}/export"
    params = {
        'TYPE': 'league',
        'L': league_id,
        'APIKEY': api_key,
        'JSON': '1'
    }
    
    # Basic retry with backoff to handle intermittent network/server issues
    last_err = None
    for attempt in range(3):
        try:
            response = requests.get(url, params=params, timeout=20)
            response.raise_for_status()
            return response.json()
        except requests.RequestException as e:
            last_err = e
            # Short backoff before retrying
            time.sleep(0.5 * (attempt + 1))
    # If all retries failed, re-raise the last error
    raise last_err


def _to_list(maybe_list):
    """Ensure the input is a list (MFL sometimes returns dict when count=1)."""
    if maybe_list is None:
        return []
    if isinstance(maybe_list, list):
        return maybe_list
    return [maybe_list]


def _parse_limit_range(limit_val: Union[str, int, float, None]):
    """
    Parse a limit string like '1-2' or '2' into (min, max) ints. Returns (None, None) if unknown.
    """
    if limit_val is None:
        return None, None
    try:
        s = str(limit_val).strip()
        if '-' in s:
            lo, hi = s.split('-', 1)
            return int(lo), int(hi)
        # single number
        v = int(s)
        return v, v
    except Exception:
        return None, None


def create_roster_info_table(league_data: dict) -> pd.DataFrame:
    """
    Build a per-position roster info table including:
      - rosterLimits min/max for each position
      - starters min/max for each position
      - totals from starters: iop_starters, count, idp_starters

    Returns a DataFrame with one row per position and totals duplicated across rows.
    """
    league = league_data.get('league', {})

    # Extract totals
    starters_meta = league.get('starters', {}) if isinstance(league.get('starters'), dict) else {}
    iop_total = starters_meta.get('iop_starters')
    starters_count_total = starters_meta.get('count')
    idp_total = starters_meta.get('idp_starters')

    # Extract positions from rosterLimits and starters
    rl_positions = _to_list(league.get('rosterLimits', {}).get('position'))
    st_positions = _to_list(starters_meta.get('position'))

    # Build lookups by position name
    rl_lookup = {}
    for item in rl_positions:
        try:
            pos = item.get('name')
            rl_min, rl_max = _parse_limit_range(item.get('limit'))
            if pos:
                rl_lookup[pos] = (rl_min, rl_max)
        except Exception:
            continue

    st_lookup = {}
    for item in st_positions:
        try:
            pos = item.get('name')
            st_min, st_max = _parse_limit_range(item.get('limit'))
            if pos:
                st_lookup[pos] = (st_min, st_max)
        except Exception:
            continue

    # Union of positions found
    all_positions = sorted(set(rl_lookup.keys()) | set(st_lookup.keys()))

    rows = []
    # Normalize totals to int where possible
    def _to_int(v):
        try:
            return int(str(v)) if v is not None and str(v).strip() != '' else None
        except Exception:
            return None

    iop_total_i = _to_int(iop_total)
    starters_count_i = _to_int(starters_count_total)
    idp_total_i = _to_int(idp_total)

    for pos in all_positions:
        rl_min, rl_max = rl_lookup.get(pos, (None, None))
        st_min, st_max = st_lookup.get(pos, (None, None))
        rows.append({
            'position': pos,
            'roster_min': rl_min,
            'roster_max': rl_max,
            'starters_min': st_min,
            'starters_max': st_max,
            'iop_starters_total': iop_total_i,
            'starters_count_total': starters_count_i,
            'idp_starters_total': idp_total_i,
        })

    return pd.DataFrame(rows)

def create_franchises_table(league_data):
    """
    Create franchises table with division/conference info merged in
    
    Args:
        league_data (dict): The JSON response from the API
    
    Returns:
        pd.DataFrame: Complete franchises table with division info
    """
    league_info = league_data['league']
    franchises = league_info['franchises']['franchise']
    
    # Create divisions lookup
    division_lookup = {}
    if 'divisions' in league_info and 'conferences' in league_info:
        divisions = league_info['divisions']['division']
        conferences = league_info['conferences']['conference']
        
        # Create conference lookup
        conf_lookup = {conf['id']: conf['name'] for conf in conferences}
        
        # Create division lookup with conference names
        for div in divisions:
            division_lookup[div['id']] = {
                'division_name': div['name'],
                'conference_id': div['conference'],
                'conference_name': conf_lookup.get(div['conference'], '')
            }
    
    # All franchise fields
    franchise_fields = [
        'id', 'name', 'abbrev', 'owner_name', 'email', 'division',
        'salaryCapAmount', 'waiverSortOrder', 'lastVisit', 'country', 
        'state', 'city', 'zip', 'phone', 'cell', 'cell2', 'address',
        'future_draft_picks', 'logo', 'icon', 'stadium', 'sound',
        'mail_event', 'time_zone', 'use_advanced_editor', 'username',
        'play_audio', 'twitterUsername'
    ]
    
    # Extract franchise data
    franchise_list = []
    for franchise in franchises:
        franchise_record = {}
        
        # Extract all franchise fields
        for field in franchise_fields:
            value = franchise.get(field)
            if value is not None:
                franchise_record[field] = str(value).strip() if isinstance(value, str) else value
            else:
                franchise_record[field] = None
        
        # Add division/conference info
        div_id = franchise.get('division')
        if div_id in division_lookup:
            franchise_record['division_name'] = division_lookup[div_id]['division_name']
            franchise_record['conference_id'] = division_lookup[div_id]['conference_id']
            franchise_record['conference_name'] = division_lookup[div_id]['conference_name']
        else:
            franchise_record['division_name'] = None
            franchise_record['conference_id'] = None
            franchise_record['conference_name'] = None
        
        # Parse draft picks
        draft_picks = franchise.get('future_draft_picks', '')
        if draft_picks:
            picks_2026 = draft_picks.count('2026')
            picks_2027 = draft_picks.count('2027')
            franchise_record['draft_picks_2026_count'] = picks_2026
            franchise_record['draft_picks_2027_count'] = picks_2027
            franchise_record['total_future_picks'] = picks_2026 + picks_2027
        else:
            franchise_record['draft_picks_2026_count'] = 0
            franchise_record['draft_picks_2027_count'] = 0
            franchise_record['total_future_picks'] = 0
        
        # Convert numeric fields
        if franchise_record['salaryCapAmount']:
            try:
                franchise_record['salaryCapAmount_numeric'] = float(franchise_record['salaryCapAmount'])
            except (ValueError, TypeError):
                franchise_record['salaryCapAmount_numeric'] = None
        else:
            franchise_record['salaryCapAmount_numeric'] = None
            
        if franchise_record['waiverSortOrder']:
            try:
                franchise_record['waiverSortOrder_numeric'] = int(franchise_record['waiverSortOrder'])
            except (ValueError, TypeError):
                franchise_record['waiverSortOrder_numeric'] = None
        else:
            franchise_record['waiverSortOrder_numeric'] = None
        
        franchise_list.append(franchise_record)
    
    return pd.DataFrame(franchise_list)

def create_league_settings_table(league_data):
    """
    Create league settings table
    
    Args:
        league_data (dict): The JSON response from the API
    
    Returns:
        pd.DataFrame: League settings as a single-row table
    """
    league_info = league_data['league']
    
    settings = {
        'league_name': league_info.get('name'),
        'league_id': league_info.get('id'),
        'start_week': league_info.get('startWeek'),
        'end_week': league_info.get('endWeek'),
        'last_regular_season_week': league_info.get('lastRegularSeasonWeek'),
        'roster_size': league_info.get('rosterSize'),
        'taxi_squad': league_info.get('taxiSquad'),
        'injured_reserve': league_info.get('injuredReserve'),
        'uses_salaries': league_info.get('usesSalaries'),
        'salary_cap_amount': league_info.get('salaryCapAmount'),
        'min_bid': league_info.get('minBid'),
        'bid_increment': league_info.get('bidIncrement'),
        'h2h': league_info.get('h2h'),
        'best_lineup': league_info.get('bestLineup'),
        'survivor_pool': league_info.get('survivorPool'),
        'fantasy_pool_type': league_info.get('fantasyPoolType'),
        'fantasy_pool_start_week': league_info.get('fantasyPoolStartWeek'),
        'fantasy_pool_end_week': league_info.get('fantasyPoolEndWeek'),
        'survivor_pool_start_week': league_info.get('survivorPoolStartWeek'),
        'survivor_pool_end_week': league_info.get('survivorPoolEndWeek'),
        'victory_points_win': league_info.get('victoryPointsWin'),
        'victory_points_loss': league_info.get('victoryPointsLoss'),
        'victory_points_tie': league_info.get('victoryPointsTie'),
        'victory_points_start_week': league_info.get('victoryPointsStartWeek'),
        'victory_points_end_week': league_info.get('victoryPointsEndWeek'),
        'victory_points_buckets': league_info.get('victoryPointsBuckets'),
        'current_waiver_type': league_info.get('currentWaiverType'),
        'max_waiver_rounds': league_info.get('maxWaiverRounds'),
        'draft_player_pool': league_info.get('draftPlayerPool'),
        'draft_timer_susp': league_info.get('draftTimerSusp'),
        'draft_limit_hours': league_info.get('draftLimitHours'),
        'draft_kind': league_info.get('draft_kind'),
        'auction_kind': league_info.get('auction_kind'),
        'load_rosters': league_info.get('loadRosters'),
        'draft_timer': league_info.get('draftTimer'),
        'precision': league_info.get('precision'),
        'lockout': league_info.get('lockout'),
        'partial_lineup_allowed': league_info.get('partialLineupAllowed'),
        'uses_contract_year': league_info.get('usesContractYear'),
        'rosters_per_player': league_info.get('rostersPerPlayer'),
        'player_limit_unit': league_info.get('playerLimitUnit'),
        'standings_sort': league_info.get('standingsSort'),
        'default_trade_expiration_days': league_info.get('defaultTradeExpirationDays'),
        'include_ir_with_salary': league_info.get('includeIRWithSalary'),
        'mobile_alerts': league_info.get('mobileAlerts'),
        'base_url': league_info.get('baseURL'),
        'franchise_count': league_info.get('franchises', {}).get('count'),
        'division_count': league_info.get('divisions', {}).get('count'),
        'conference_count': league_info.get('conferences', {}).get('count')
    }
    
    # Convert to single-row DataFrame
    return pd.DataFrame([settings])


def _ensure_dir(path: str):
    """Create directory if it doesn't exist."""
    os.makedirs(path, exist_ok=True)


def _save_per_year_and_update_combined(df: pd.DataFrame, year: Union[int, str], out_dir: str, per_year_prefix: str, combined_name: str):
    """
    Save a per-year CSV (with season column) and regenerate a combined CSV from all per-year files.

    Args:
        df: Dataframe to save.
        year: Season year to assign to 'season' column and filename.
        out_dir: Directory where CSVs are stored.
        per_year_prefix: Filename prefix for per-year files (e.g., 'leagueInfo_' or 'franchises_').
        combined_name: Filename for the combined CSV (e.g., 'leagueInfo_all.csv').
    """
    _ensure_dir(out_dir)

    # Ensure season column exists and is the first column for readability
    season_val = int(year)
    df = df.copy()
    df['season'] = season_val
    # Reorder columns: season first
    cols = ['season'] + [c for c in df.columns if c != 'season']
    df = df[cols]

    per_year_path = os.path.join(out_dir, f"{per_year_prefix}{season_val}.csv")
    df.to_csv(per_year_path, index=False)

    # Rebuild combined from all per-year files present
    pattern = os.path.join(out_dir, f"{per_year_prefix}*.csv")
    per_year_files = sorted(
        fp for fp in glob.glob(pattern)
        if os.path.basename(fp) != combined_name
    )
    if per_year_files:
        frames = []
        for fp in per_year_files:
            try:
                frames.append(pd.read_csv(fp))
            except Exception as e:
                print(f"Warning: could not read {fp}: {e}")
        if frames:
            combined_df = pd.concat(frames, ignore_index=True, sort=False)
            combined_path = os.path.join(out_dir, combined_name)
            combined_df.to_csv(combined_path, index=False)
            print(f"Updated combined file: {combined_path} ({len(combined_df)} rows)")

def process_single_year(year, league_id="60206", api_key=""):
    """
    Process a single year and return both tables
    
    Args:
        year (str/int): The season year
        league_id (str): League ID
        api_key (str): API key
    
    Returns:
        tuple: (franchises_df, settings_df)
    """
    print(f"Processing year {year}...")
    
    # Fetch data
    data = fetch_league_data(year, league_id, api_key)
    
    # Create tables
    franchises_df = create_franchises_table(data)
    settings_df = create_league_settings_table(data)
    roster_info_df = create_roster_info_table(data)

    # Output directories and filenames
    repo_root = os.path.abspath(os.path.join(os.path.dirname(__file__), os.pardir))
    league_info_dir = os.path.join(repo_root, 'data', 'leagueInfo')
    franchises_dir = os.path.join(repo_root, 'data', 'franchises')
    roster_info_dir = os.path.join(repo_root, 'data', 'rosterInfo')

    # Save per-year and update combined for league info
    _save_per_year_and_update_combined(
        df=settings_df,
        year=year,
        out_dir=league_info_dir,
        per_year_prefix='leagueInfo_',
        combined_name='leagueInfo_all.csv'
    )

    # Save per-year and update combined for franchises
    _save_per_year_and_update_combined(
        df=franchises_df,
        year=year,
        out_dir=franchises_dir,
        per_year_prefix='franchises_',
        combined_name='franchises_all.csv'
    )

    # Save per-year and update combined for roster info
    _save_per_year_and_update_combined(
        df=roster_info_df,
        year=year,
        out_dir=roster_info_dir,
        per_year_prefix='rosterInfo_',
        combined_name='rosterInfo_all.csv'
    )

    print(f"Saved season {year}: leagueInfo, franchises, and rosterInfo (per-year and combined)")

    return franchises_df, settings_df

def process_multiple_years(years, league_id="60206", api_key=""):
    """
    Process multiple years
    
    Args:
        years (list): List of years to process
        league_id (str): League ID
        api_key (str): API key
    """
    for year in years:
        try:
            process_single_year(year, league_id, api_key)
        except Exception as e:
            print(f"Error processing year {year}: {e}")

# Example usage
if __name__ == "__main__":
    
    '''
    # Single year
    year = 2025
    franchises_df, settings_df = process_single_year(year)
    
    print(f"\nFranchises table shape: {franchises_df.shape}")
    print(f"Settings table shape: {settings_df.shape}")
    
    print(f"\nSample franchise data:")
    print(franchises_df[['name', 'owner_name', 'division_name', 'conference_name']].head())
    '''
    # Multiple years
    years_to_process = range(2018, 2026)
    process_multiple_years(years_to_process)