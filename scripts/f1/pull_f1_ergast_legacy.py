#!/usr/bin/env python3
import json
import os
import sys
import time
from pathlib import Path
from typing import Optional

import requests

# Jolpica (Ergast successor) base:
# Example endpoint: https://api.jolpica.ca/ergast/f1/1950.json
BASE = os.getenv("ERGAST_BASE_URL", "https://api.jolpica.ca/ergast/f1").rstrip("/")
OUT_ROOT = Path(os.getenv("ERGAST_OUT_ROOT", "data/f1"))

START_YEAR = int(os.getenv("ERGAST_START_YEAR", "1950"))
END_YEAR = int(os.getenv("ERGAST_END_YEAR", "1959"))

# Best option for your use-case (Xcode year filtering):
# - One results.json per year (few requests, avoids 429)
DOWNLOAD_YEAR_RESULTS = os.getenv("ERGAST_DOWNLOAD_YEAR_RESULTS", "true").lower() == "true"

# Optional legacy mode (many requests -> may 429). Keep OFF unless you really need per-round files.
DOWNLOAD_RESULTS_PER_ROUND = os.getenv("ERGAST_DOWNLOAD_RESULTS_PER_ROUND", "false").lower() == "true"

# Base sleep between successful requests (still recommended even with retries)
SLEEP_SECONDS = float(os.getenv("ERGAST_SLEEP_SECONDS", "0.35"))

# Retry behavior for rate limiting / transient failures
MAX_RETRIES = int(os.getenv("ERGAST_MAX_RETRIES", "8"))
BACKOFF_BASE_SECONDS = float(os.getenv("ERGAST_BACKOFF_BASE_SECONDS", "1.0"))

SESSION = requests.Session()
SESSION.headers.update({"User-Agent": "pl-data-backfill/1.1"})

def ensure_dir(p: Path) -> None:
    p.mkdir(parents=True, exist_ok=True)

def write_json(path: Path, payload: dict) -> None:
    ensure_dir(path.parent)
    path.write_text(json.dumps(payload, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
    print(f"✅ wrote {path}")

def safe_sleep(seconds: float = SLEEP_SECONDS) -> None:
    if seconds > 0:
        time.sleep(seconds)

def _retry_after_seconds(resp: requests.Response) -> Optional[float]:
    ra = resp.headers.get("Retry-After")
    if not ra:
        return None
    try:
        return float(ra)
    except Exception:
        return None

def fetch_json(url: str) -> dict:
    """
    Robust fetch with retries for 429 + transient 5xx.
    Uses Retry-After when present, else exponential backoff.
    """
    last_err: Optional[Exception] = None

    for attempt in range(0, MAX_RETRIES + 1):
        try:
            resp = SESSION.get(url, timeout=60)

            # Success
            if 200 <= resp.status_code < 300:
                return resp.json()

            # Rate limit
            if resp.status_code == 429:
                wait = _retry_after_seconds(resp)
                if wait is None:
                    wait = BACKOFF_BASE_SECONDS * (2 ** attempt)
                print(f"⏳ 429 Too Many Requests. Waiting {wait:.1f}s then retrying: {url}", file=sys.stderr)
                safe_sleep(wait)
                continue

            # Transient server errors
            if 500 <= resp.status_code < 600:
                wait = BACKOFF_BASE_SECONDS * (2 ** attempt)
                print(f"⏳ HTTP {resp.status_code}. Waiting {wait:.1f}s then retrying: {url}", file=sys.stderr)
                safe_sleep(wait)
                continue

            # Other client errors: fail fast with details
            try:
                body = resp.text[:800]
            except Exception:
                body = "<no body>"
            raise RuntimeError(f"HTTP {resp.status_code} for {url}\n{body}")

        except Exception as e:
            last_err = e
            if attempt >= MAX_RETRIES:
                break
            wait = BACKOFF_BASE_SECONDS * (2 ** attempt)
            print(f"⚠️ Request error: {e}. Waiting {wait:.1f}s then retrying: {url}", file=sys.stderr)
            safe_sleep(wait)

    raise RuntimeError(f"Failed after retries for {url}. Last error: {last_err}")

def rounds_from_races_payload(races_payload: dict) -> list[int]:
    """
    Jolpica/Ergast structure: MRData -> RaceTable -> Races[] with "round"
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

    # Safety: per-round + per-year results both ON is redundant and increases load.
    if DOWNLOAD_RESULTS_PER_ROUND and DOWNLOAD_YEAR_RESULTS:
        print(
            "NOTE: Both ERGAST_DOWNLOAD_YEAR_RESULTS and ERGAST_DOWNLOAD_RESULTS_PER_ROUND are true.\n"
            "      This is redundant and increases API calls. Consider turning PER_ROUND off.",
            file=sys.stderr,
        )

    for year in range(START_YEAR, END_YEAR + 1):
        year_dir = OUT_ROOT / str(year)
        ensure_dir(year_dir)

        # races (calendar)
        races_url = f"{BASE}/{year}.json?limit=1000"
        races = fetch_json(races_url)
        write_json(year_dir / "races.json", races)
        safe_sleep()

        # driver standings
        sd_url = f"{BASE}/{year}/driverStandings.json?limit=1000"
        standings_drivers = fetch_json(sd_url)
        write_json(year_dir / "standings_drivers.json", standings_drivers)
        safe_sleep()

        # constructor standings
        st_url = f"{BASE}/{year}/constructorStandings.json?limit=1000"
        standings_teams = fetch_json(st_url)
        write_json(year_dir / "standings_teams.json", standings_teams)
        safe_sleep()

        # ✅ Best option: ONE results file per year
        if DOWNLOAD_YEAR_RESULTS:
            results_url = f"{BASE}/{year}/results.json?limit=1000"
            results = fetch_json(results_url)
            write_json(year_dir / "results.json", results)
            safe_sleep()

        # Optional legacy: per-round results (many requests)
        if DOWNLOAD_RESULTS_PER_ROUND:
            rr_dir = year_dir / "race_results"
            ensure_dir(rr_dir)

            rounds = rounds_from_races_payload(races)
            for rd in rounds:
                res_url = f"{BASE}/{year}/{rd}/results.json?limit=1000"
                payload = fetch_json(res_url)
                write_json(rr_dir / f"{rd}.json", payload)
                safe_sleep()

    print("✅ Legacy backfill complete.")

if __name__ == "__main__":
    main()
