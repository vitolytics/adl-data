"""
Downloads the DynastyProcess player-id crosswalk and writes one current CSV.

Source:
https://github.com/dynastyprocess/data/blob/master/files/db_playerids.csv

DynastyProcess rebuilds this file weekly (Fridays at 00:23 UTC). This repo
refreshes its copy every other day; a run with no upstream change leaves the
file untouched. It is one row per player. mfl_id is unique and matches the MFL
player id used everywhere else
in this repo. The other *_id columns are that same player on Sleeper, ESPN,
Yahoo, PFF, and the rest, which is how data from those sites gets joined back.
The literal token NA in the source means that site has no id for the player.

Also included: name, position, team, birthdate, age, draft slot, height,
weight, and college.

Output:
- data/playerIds/playerIds.csv

No API key is required. Missing values are written as empty cells.
"""

from __future__ import annotations

import io
import os
import time

import pandas as pd
import requests


SOURCE_URL = "https://raw.githubusercontent.com/dynastyprocess/data/master/files/db_playerids.csv"

# Column order published by DynastyProcess as of 2026. New columns they add
# later are kept and appended. A missing column from this list fails the run
# so a renamed id does not disappear quietly.
EXPECTED_COLUMNS = [
    "mfl_id",
    "sportradar_id",
    "fantasypros_id",
    "gsis_id",
    "pff_id",
    "sleeper_id",
    "nfl_id",
    "espn_id",
    "yahoo_id",
    "fleaflicker_id",
    "cbs_id",
    "pfr_id",
    "cfbref_id",
    "rotowire_id",
    "rotoworld_id",
    "ktc_id",
    "stats_id",
    "stats_global_id",
    "fantasy_data_id",
    "swish_id",
    "name",
    "merge_name",
    "position",
    "team",
    "birthdate",
    "age",
    "draft_year",
    "draft_round",
    "draft_pick",
    "draft_ovr",
    "twitter_username",
    "height",
    "weight",
    "college",
    "db_season",
]

# A real pull is about 12,000 players. Far fewer means the download was truncated
# or the response was not the CSV.
MIN_ROWS = 1000


def _repo_root() -> str:
    return os.path.abspath(os.path.join(os.path.dirname(__file__), os.pardir))


def fetch_player_ids() -> str:
    """Download the crosswalk CSV. Returns the response body as text."""
    last_err: Exception | None = None
    for attempt in range(3):
        try:
            resp = requests.get(SOURCE_URL, timeout=60)
            resp.raise_for_status()
            text = resp.text
            if not text.startswith("mfl_id,"):
                raise ValueError("Response was not the player-id CSV (missing mfl_id header)")
            return text
        except (requests.RequestException, ValueError) as e:
            last_err = e
            time.sleep(1.0 * (attempt + 1))
    raise RuntimeError(f"Failed to download player ids from {SOURCE_URL}: {last_err}")


def normalize_player_ids(text: str) -> pd.DataFrame:
    """Parse the CSV. Every value stays a string so ids are never rewritten as numbers."""
    df = pd.read_csv(
        io.StringIO(text),
        dtype=str,
        keep_default_na=False,
        na_values=["NA"],
    )
    missing = [c for c in EXPECTED_COLUMNS if c not in df.columns]
    if missing:
        raise ValueError(f"Player-id file is missing columns: {missing}")

    extra = [c for c in df.columns if c not in EXPECTED_COLUMNS]
    df = df[EXPECTED_COLUMNS + extra]
    df = df.apply(lambda col: col.str.strip())
    df = df.mask(df == "")

    blank_mfl = df["mfl_id"].isna()
    if blank_mfl.any():
        print(f"  dropping {int(blank_mfl.sum())} rows with no mfl_id")
        df = df.loc[~blank_mfl].copy()

    duplicated = df["mfl_id"].duplicated()
    if duplicated.any():
        samples = df.loc[duplicated, "mfl_id"].unique()[:10].tolist()
        raise ValueError(f"Duplicate mfl_id values: {samples}")

    if len(df) < MIN_ROWS:
        raise ValueError(f"Player-id file has {len(df)} rows; expected at least {MIN_ROWS}")

    order = pd.to_numeric(df["mfl_id"], errors="coerce")
    df = df.assign(_ord=order).sort_values(["_ord", "mfl_id"], kind="mergesort")
    df = df.drop(columns="_ord").reset_index(drop=True)
    return df


def save_player_ids(df: pd.DataFrame) -> str:
    out_dir = os.path.join(_repo_root(), "data", "playerIds")
    os.makedirs(out_dir, exist_ok=True)
    out_path = os.path.join(out_dir, "playerIds.csv")
    df.to_csv(out_path, index=False, lineterminator="\n")
    return out_path


if __name__ == "__main__":
    print(f"Downloading player ids from {SOURCE_URL}")
    frame = normalize_player_ids(fetch_player_ids())
    path = save_player_ids(frame)
    filled = frame.notna().sum()
    print(f"  rows: {len(frame)}")
    print(f"  columns: {len(frame.columns)}")
    for col in ("mfl_id", "sleeper_id", "espn_id", "gsis_id", "pff_id", "name"):
        if col in filled.index:
            print(f"  {col}: {int(filled[col])} filled")
    print(f"Saved {path}")
