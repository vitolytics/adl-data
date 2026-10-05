"""
Resolves the current MFL season and NFL week directly from the MFL API so no
season/week value has to be maintained by hand.

Season rule:
- The current season is the newest year (this calendar year, else last year) in which
  the league exists on MFL. MFL answers HTTP 404 for a year it has not opened yet and
  HTTP 200 with an {"error": ...} body when the year is open but the league has not been
  renewed into it. Any other failure (timeouts, 5xx) raises, so a transient outage can
  never cause current data to be written into the previous season's files.

Week rule:
- The current week is the week MFL's nflSchedule export reports for the season.

API examples:
- https://api.myfantasyleague.com/{year}/export?TYPE=league&L=...&JSON=1
- https://api.myfantasyleague.com/{year}/export?TYPE=nflSchedule&JSON=1
"""

from __future__ import annotations

import time
from datetime import date
from typing import Any, Dict, Optional

import requests


class SeasonDetectionError(RuntimeError):
    pass


def _get(url: str, params: Dict[str, str]) -> requests.Response:
    last_err: Optional[Exception] = None
    for attempt in range(3):
        try:
            resp = requests.get(url, params=params, timeout=20)
            if resp.status_code == 404 or resp.ok:
                return resp
            last_err = requests.HTTPError(f"HTTP {resp.status_code} for {resp.url}")
        except requests.RequestException as e:
            last_err = e
        time.sleep(1.0 * (attempt + 1))
    raise SeasonDetectionError(f"MFL request failed after retries: {last_err}")


def league_exists(year: int, league_id: str, api_key: str = "") -> bool:
    params = {'TYPE': 'league', 'L': league_id, 'JSON': '1'}
    if api_key:
        params['APIKEY'] = api_key
    resp = _get(f"https://api.myfantasyleague.com/{year}/export", params)
    if resp.status_code == 404:
        return False
    try:
        data: Dict[str, Any] = resp.json()
    except ValueError as e:
        raise SeasonDetectionError(f"Non-JSON response from MFL for {year}: {e}")
    if 'league' in data:
        return True
    if 'error' in data:
        return False
    raise SeasonDetectionError(f"Unexpected MFL league response for {year}: {list(data.keys())}")


def detect_current_season(league_id: str, api_key: str = "", today: Optional[date] = None) -> int:
    year = (today or date.today()).year
    for candidate in (year, year - 1):
        if league_exists(candidate, league_id, api_key):
            print(f"Detected current MFL season: {candidate}")
            return candidate
    raise SeasonDetectionError(f"League {league_id} not found on MFL for {year} or {year - 1}")


def detect_current_week(season: int) -> int:
    resp = _get(
        f"https://api.myfantasyleague.com/{season}/export",
        {'TYPE': 'nflSchedule', 'JSON': '1'},
    )
    if resp.status_code == 404:
        raise SeasonDetectionError(f"MFL has no NFL schedule for {season}")
    week = resp.json().get('nflSchedule', {}).get('week')
    try:
        week_i = int(str(week).strip())
    except Exception:
        raise SeasonDetectionError(f"MFL nflSchedule returned no week for {season}: {week!r}")
    print(f"Detected current NFL week for {season}: {week_i}")
    return week_i
