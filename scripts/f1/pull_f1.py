#!/usr/bin/env python3
import json
import os
import sys
from pathlib import Path
from urllib.parse import urlencode

import requests

BASE_URL = os.getenv("F1_BASE_URL", "https://v1.formula-1.api-sports.io")
API_KEY = os.getenv("APISPORTS_KEY") or os.getenv("F1_API_KEY")
YEAR = os.getenv("F1_SEASON", "2024")
OUT_DIR = Path(os.getenv("F1_OUT_DIR", f"data/f1/{YEAR}"))

# Optional: if you later want race result files per race id
ENABLE_RACE_RESULTS = os.getenv("F1_ENABLE_RACE_RESULTS", "false").lower() == "true"
RACE_RESULTS_GET = os.getenv("F1_RACE_RESULTS_GET", "races/results")  # change if your API uses a different "get"
RACE_RESULTS_PARAM = os.getenv("F1_RACE_RESULTS_PARAM", "race")       # usually "race" or "id"

if not API_KEY:
    print("ERROR: Missing API key. Set APISPORTS_KEY (recommended) or F1_API_KEY in env.", file=sys.stderr)
    sys.exit(1)

HEADERS = {
    # API-Sports accepts this header on direct domain
    "x-apisports-key": API_KEY,
    # Some setups also work with these (harmless if ignored)
    "x-rapidapi-key": API_KEY,
}

def ensure_dir(p: Path) -> None:
    p.mkdir(parents=True, exist_ok=True)

def fetch_json(get_name: str, params: dict | None = None) -> dict:
    params = params or {}
    url = f"{BASE_URL}/{get_name}"
    if params:
        url = f"{url}?{urlencode(params)}"

    r = requests.get(url, headers=HEADERS, timeout=45)
    try:
        payload = r.json()
    except Exception:
        print(f"ERROR: Non-JSON response from {url}. Status={r.status_code}\n{r.text[:500]}", file=sys.stderr)
        raise

    if r.status_code >= 400:
        print(f"ERROR: HTTP {r.status_code} for {url}\n{json.dumps(payload, indent=2)[:1500]}", file=sys.stderr)
        raise RuntimeError(f"HTTP {r.status_code}")

    return payload

def write_json(path: Path, payload: dict) -> None:
    ensure_dir(path.parent)
    path.write_text(json.dumps(payload, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
    print(f"✅ wrote {path}")

def first_error(payload: dict) -> str | None:
    err = payload.get("errors")
    if isinstance(err, dict) and err:
        # return first error message
        k = next(iter(err.keys()))
        return f"{k}: {err.get(k)}"
    return None

def main() -> None:
    ensure_dir(OUT_DIR)

    # ---- IMPORTANT PART ----
    # teams/drivers/circuits do NOT support season
    # races + standings DO support season

    jobs = [
        # "seasons" is often the list of available seasons (no season param)
        ("seasons", {}, OUT_DIR / "season.json"),

        # Season-specific
        ("races", {"season": YEAR}, OUT_DIR / "races.json"),
        ("rankings/drivers", {"season": YEAR}, OUT_DIR / "standings_drivers.json"),
        ("rankings/teams", {"season": YEAR}, OUT_DIR / "standings_teams.json"),

        # Master data (NO season)
        ("circuits", {}, OUT_DIR / "circuits.json"),
        ("drivers", {}, OUT_DIR / "drivers.json"),
        ("teams", {}, OUT_DIR / "teams.json"),
    ]

    for get_name, params, out_path in jobs:
        payload = fetch_json(get_name, params)
        err = first_error(payload)
        if err:
            print(f"⚠️ API returned error for {get_name} params={params}: {err}")
        write_json(out_path, payload)

    # Optional: race_results per race id (only enable once you confirm endpoint name)
    if ENABLE_RACE_RESULTS:
        races_payload = json.loads((OUT_DIR / "races.json").read_text(encoding="utf-8"))
        races = races_payload.get("response") or []
        if not isinstance(races, list):
            print("⚠️ races.json response is not a list; skipping race_results")
            return

        rr_dir = OUT_DIR / "race_results"
        ensure_dir(rr_dir)

        for race in races:
            race_id = race.get("id") or race.get("race") or race.get("race_id")
            if not race_id:
                continue
            payload = fetch_json(RACE_RESULTS_GET, {RACE_RESULTS_PARAM: race_id})
            write_json(rr_dir / f"{race_id}.json", payload)

if __name__ == "__main__":
    main()
