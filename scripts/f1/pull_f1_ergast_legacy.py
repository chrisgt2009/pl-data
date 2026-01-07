#!/usr/bin/env python3
import json
import os
import sys
import time
from pathlib import Path

import requests

BASE = os.getenv("ERGAST_BASE_URL", "http://api.jolpi.ca/ergast/f1")
OUT_ROOT = Path(os.getenv("ERGAST_OUT_ROOT", "data/f1"))

START_YEAR = int(os.getenv("ERGAST_START_YEAR", "1950"))
END_YEAR = int(os.getenv("ERGAST_END_YEAR", "2022"))

# Optional: download per-round results (slower but complete)
DOWNLOAD_RESULTS = os.getenv("ERGAST_DOWNLOAD_RESULTS", "true").lower() == "true"

# To avoid rate limits / be nice to the API
SLEEP_SECONDS = float(os.getenv("ERGAST_SLEEP_SECONDS", "0.25"))

SESSION = requests.Session()
SESSION.headers.update({"User-Agent": "pl-data-backfill/1.0"})

def ensure_dir(p: Path) -> None:
    p.mkdir(parents=True, exist_ok=True)

def fetch_json(url: str) -> dict:
    r = SESSION.get(url, timeout=60)
    r.raise_for_status()
    return r.json()

def write_json(path: Path, payload: dict) -> None:
    ensure_dir(path.parent)
    path.write_text(json.dumps(payload, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
    print(f"✅ wrote {path}")

def safe_sleep():
    if SLEEP_SECONDS > 0:
        time.sleep(SLEEP_SECONDS)

def rounds_from_races_payload(races_payload: dict) -> list[int]:
    """
    Ergast/Jolpica structure: MRData -> RaceTable -> Races[] with "round"
    """
    try:
        races = races_payload["MRData"]["RaceTable"]["Races"]
        rounds = []
        for r in races:
            rd = r.get("round")
            if rd is None:
                continue
            try:
                rounds.append(int(rd))
            except Exception:
                pass
        return sorted(set(rounds))
    except Exception:
        return []

def main() -> None:
    if START_YEAR > END_YEAR:
        print("ERROR: ERGAST_START_YEAR must be <= ERGAST_END_YEAR", file=sys.stderr)
        sys.exit(1)

    for year in range(START_YEAR, END_YEAR + 1):
        year_dir = OUT_ROOT / str(year)
        ensure_dir(year_dir)

        # races
        races_url = f"{BASE}/{year}.json?limit=1000"
        races = fetch_json(races_url)
        write_json(year_dir / "races.json", races)
        safe_sleep()

        # standings (drivers)
        sd_url = f"{BASE}/{year}/driverStandings.json?limit=1000"
        standings_drivers = fetch_json(sd_url)
        write_json(year_dir / "standings_drivers.json", standings_drivers)
        safe_sleep()

        # standings (teams/constructors)
        st_url = f"{BASE}/{year}/constructorStandings.json?limit=1000"
        standings_teams = fetch_json(st_url)
        write_json(year_dir / "standings_teams.json", standings_teams)
        safe_sleep()

        # optional per-round results
        if DOWNLOAD_RESULTS:
            rounds = rounds_from_races_payload(races)
            rr_dir = year_dir / "race_results"
            ensure_dir(rr_dir)

            for rd in rounds:
                # per-round results
                res_url = f"{BASE}/{year}/{rd}/results.json?limit=1000"
                payload = fetch_json(res_url)
                write_json(rr_dir / f"{rd}.json", payload)
                safe_sleep()

    print("✅ Legacy backfill complete.")

if __name__ == "__main__":
    main()
