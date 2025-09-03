"""
Ingests Contract Admin salaries from a Google Sheet and writes normalized CSVs.

Source:
- Google Sheet ID: 1H7iSTO_485ptLc6RJcLjDC1B7258b9HPle13BsMRPu8
- Tab: "Jul1 Sal"

Layout pattern (repeats across columns left-to-right):
- Row 1 contains a position label over each cluster.
- For each position cluster, the next 3 columns are: Player, Conf, Sal, then 1 blank spacer column.

Output:
- Writes to data/salaries/contractAdmin/
  - contractAdmin_asof_YYYY-MM-DD_HHMM.csv (timestamped snapshot)
  - contractAdmin_latest.csv (overwritten latest snapshot)

Dependencies: pandas, requests (already in requirements.txt)
"""

from __future__ import annotations

import os
import re
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd
import requests
from urllib.parse import quote


SHEET_ID = "1H7iSTO_485ptLc6RJcLjDC1B7258b9HPle13BsMRPu8"
SHEET_TAB = "Jul1 Sal"


def _repo_root() -> str:
    return os.path.abspath(os.path.join(os.path.dirname(__file__), os.pardir))


def _ensure_dir(p: str) -> None:
    os.makedirs(p, exist_ok=True)


def _download_sheet_csv(sheet_id: str, sheet_tab: str, timeout: int = 30) -> bytes:
    """Download a sheet tab as CSV using the gviz endpoint by tab name.

    This typically works for sheets that are viewable with the link.
    """
    base = f"https://docs.google.com/spreadsheets/d/{sheet_id}/gviz/tq"
    params = f"tqx=out:csv&sheet={quote(sheet_tab)}"
    url = f"{base}?{params}"
    resp = requests.get(url, timeout=timeout)
    resp.raise_for_status()
    if not resp.content:
        raise RuntimeError("Empty response when fetching sheet CSV")
    return resp.content


def _get_sheet_title(sheet_id: str, timeout: int = 30) -> str:
    """Fetch the human-readable Google Sheet name by scraping the HTML <title>.

    Falls back to the sheet_id when the title cannot be determined.
    """
    try:
        url = f"https://docs.google.com/spreadsheets/d/{sheet_id}/edit"
        resp = requests.get(url, timeout=timeout)
        resp.raise_for_status()
        html = resp.text
        m = re.search(r"<title>(.*?)</title>", html, flags=re.IGNORECASE | re.DOTALL)
        if m:
            title = m.group(1).strip()
            # Typically: "<Sheet Name> - Google Sheets"
            if title.lower().endswith(" - google sheets"):
                title = title[: -len(" - Google Sheets")]
            return title.strip()
    except Exception:
        pass
    return sheet_id


def _clean_salary(val: Any) -> Optional[float]:
    if val is None:
        return None
    s = str(val).strip()
    if s == "" or s.lower() in {"nan", "none"}:
        return None
    # Remove currency symbols, commas, spaces
    s = re.sub(r"[,$\s]", "", s)
    # Handle parentheses as negative, e.g., (100) => -100
    if s.startswith("(") and s.endswith(")"):
        s = "-" + s[1:-1]
    try:
        return float(s)
    except Exception:
        return None


def _is_header_token(val: Any) -> bool:
    if val is None:
        return False
    s = str(val).strip().lower()
    return s in {"player", "conf", "conference", "sal", "salary"}


def _normalize_position(label: Any) -> Optional[str]:
    if label is None:
        return None
    s = str(label).strip()
    if s == "":
        return None
    s_up = s.upper()
    # Remove common suffixes like " Player"
    if s_up.endswith(" PLAYER"):
        s_up = s_up[: -len(" PLAYER")]
    # Map synonyms
    synonyms = {
        "PK/PN": "PK/PN",
        "PK": "PK",
        "PN": "PN",
        "QB": "QB",
        "RB": "RB",
        "WR": "WR",
        "TE": "TE",
        "DT": "DT",
        "DE": "DE",
        "DL": "DL",
        "LB": "LB",
        "CB": "CB",
        "S": "S",
        "DB": "DB",
    }
    # Accept combined like "PK / PN"
    s_up = s_up.replace(" ", "")
    if s_up in {"PK/PN", "PKPN"}:
        s_up = "PK/PN"
    # Reject obvious non-positions
    if s_up in {"RANK", "AV", "AVG", "TOTAL", "TEAM", "OWNER"}:
        return None
    return synonyms.get(s_up, s_up if s_up in synonyms else None)


def parse_contract_admin_layout(df_raw: pd.DataFrame) -> pd.DataFrame:
    """Parse the repeating [Player, Conf, Sal, (blank)] clusters per position.

    Assumptions:
    - Row 0 has position labels scattered across columns at the start of each cluster.
    - Each cluster is 4 columns wide (Player, Conf, Sal, blank). The last blank may be missing for the final cluster.
    - Data starts on row >= 1. If a row begins with header tokens (Player/Conf/Sal), that row is skipped.
    """
    if df_raw.shape[0] == 0:
        return pd.DataFrame(columns=["position", "player", "conference", "salary"])  # empty

    # Treat first row as cluster-position markers
    pos_row = df_raw.iloc[0, :]
    ncols = df_raw.shape[1]

    clusters: List[Tuple[str, int, int, int]] = []  # (position, col_player, col_conf, col_sal)
    c = 0
    while c < ncols:
        label = pos_row.iloc[c]
        pos_norm = _normalize_position(label)
        if pos_norm:
            col_player = c
            col_conf = c + 1 if c + 1 < ncols else None
            col_sal = c + 2 if c + 2 < ncols else None
            if col_conf is not None and col_sal is not None:
                clusters.append((pos_norm, col_player, col_conf, col_sal))
            # Advance by 4 to next expected cluster start
            c += 4
        else:
            c += 1

    rows: List[Dict[str, Any]] = []
    # Data starts after position row
    for r in range(1, df_raw.shape[0]):
        for pos, col_p, col_c, col_s in clusters:
            try:
                player = df_raw.iat[r, col_p] if col_p < ncols else None
                conf = df_raw.iat[r, col_c] if col_c < ncols else None
                sal = df_raw.iat[r, col_s] if col_s < ncols else None
            except Exception:
                player = conf = sal = None

            # Skip empty rows
            if (
                (player is None or str(player).strip() == "")
                and (conf is None or str(conf).strip() == "")
                and (sal is None or str(sal).strip() == "")
            ):
                continue

            # Skip obvious header row(s)
            if _is_header_token(player) and _is_header_token(conf):
                continue

            player_str = str(player).strip() if player is not None else ""
            if player_str == "":
                # No player name — ignore this line
                continue

            conf_str = str(conf).strip() if conf is not None else ""
            # Normalize short forms like A/B to uppercase
            conf_norm = conf_str.upper()

            salary_val = _clean_salary(sal)
            # Require a salary value for a valid entry; this also skips Rank rows
            if salary_val is None:
                continue

            rows.append(
                {
                    "position": pos,
                    "player": player_str,
                    "conference": conf_norm,
                    "salary": salary_val,
                }
            )

    if not rows:
        return pd.DataFrame(columns=["position", "player", "conference", "salary"])  # empty

    out_df = pd.DataFrame(rows)
    # Drop rows that are entirely NaN/empty in key fields after cleaning
    out_df = out_df[~out_df["player"].isna() & (out_df["player"].str.strip() != "")]

    # Coerce salary to numeric (already float or None)
    out_df["salary"] = pd.to_numeric(out_df["salary"], errors="coerce")

    # Standardize column order
    out_df = out_df[["position", "player", "conference", "salary"]]
    return out_df.reset_index(drop=True)


def ingest_contract_admin(sheet_id: str = SHEET_ID, sheet_tab: str = SHEET_TAB) -> Tuple[str, str]:
    """Download, parse, and save Contract Admin salaries.

    Returns a tuple of (timestamped_csv_path, latest_csv_path).
    """
    csv_bytes = _download_sheet_csv(sheet_id, sheet_tab)
    # Read with header=None to keep all rows as data
    df_raw = pd.read_csv(pd.io.common.BytesIO(csv_bytes), header=None, dtype=str, keep_default_na=False)

    parsed = parse_contract_admin_layout(df_raw)
    # Attach metadata columns
    now = datetime.now()
    parsed = parsed.copy()
    parsed["asof"] = now.strftime("%Y-%m-%d %H:%M")
    parsed["source_doc"] = _get_sheet_title(sheet_id)
    parsed["tab name"] = sheet_tab

    out_dir = os.path.join(_repo_root(), "data", "salaries", "contractAdmin")
    _ensure_dir(out_dir)

    ts_name = now.strftime("%Y-%m-%d_%H%M")
    ts_path = os.path.join(out_dir, f"contractAdmin_asof_{ts_name}.csv")
    latest_path = os.path.join(out_dir, "contractAdmin_latest.csv")

    parsed.to_csv(ts_path, index=False)
    parsed.to_csv(latest_path, index=False)

    print(f"Saved: {ts_path}")
    print(f"Saved: {latest_path}")
    return ts_path, latest_path


if __name__ == "__main__":
    try:
        ingest_contract_admin()
    except Exception as e:
        print(f"Error: {e}")
        raise
