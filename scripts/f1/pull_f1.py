#!/usr/bin/env python3
import json
import os
import sys
from pathlib import Path
from urllib.parse import urlencode

import requests

BASE_URL = os.getenv("F1_BASE_URL", "https://v1.formula-1.api-sports.io")
YEAR = os.getenv("F1_SEASON", "2012")
OUT_DIR = Path(os.getenv("F1_OUT_DIR", f"data/f1/{YEAR}"))

# --- Auth (supports both API-Sports direct + RapidAPI) ---
APISPORTS_KEY = os.getenv("APISPORTS_KEY") or os.getenv("F1_API_KEY")
RAPIDAPI_KEY = os.getenv("RAPIDAPI_KEY")
RAPIDAPI_HOST = os.getenv("RAPIDAPI_HOST")  # e.g. v1.formula-1.api-sports.io

USING_RAPID = bool(RAPIDAPI_KEY)

# Optional: race results per race id
ENABLE_RACE_RESULTS = os.getenv("F1_ENABLE_RACE_RESULTS", "false").lower() == "true"
RACE_RESULTS_GET = os.getenv("F1_RACE_RESULTS_GET", "races/results")
RACE_RESULTS_PARAM = os.getenv("F1_RACE_RESULTS_PARAM", "race")

if not APISPORTS_KEY and not RAPIDAPI_KEY:
    print(
        "ERROR: Missing API key. Set APISPORTS_KEY (API-Sports) OR RAPIDAPI_KEY (RapidAPI).",
        file=sys.stderr,
    )
    sys.exit(1)

if USING_RAPID and not RAPIDAPI_HOST:
    print(
        "ERROR: RAPIDAPI_HOST is missing. Example: v1.formula-1.api-sports.io",
        file=sys.stderr,
    )
    sys.exit(1)


def build_headers() -> dict:
    """
    API-Sports direct: x-apisports-key
    RapidAPI: x-rapidapi-key + x-rapidapi-host
    """
    if USING_RAPID:
        return {
            "x-rapidapi-key": RAPIDAPI_KEY,
            "x-rapidapi-host": RAPIDAPI_HOST,
        }
    return {
        "x-apisports-key": APISPORTS_KEY,
    }


HEADERS = build_headers()


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
        print(
            f"ERROR: Non-JSON response from {url}. Status={r.status_code}\n{r.text[:500]}",
            file=sys.stderr,
        )
        raise

    if r.status_code >= 400:
        print(
            f"ERROR: HTTP {r.status_code} for {url}\n{json.dumps(payload, indent=2)[:1500]}",
            file=sys.stderr,
        )
        raise RuntimeError(f"HTTP {r.status_code}")

    return payload


def write_json(path: Path, payload: dict) -> None:
    ensure_dir(path.parent)
    path.write_text(json.dumps(payload, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
    print(f"✅ wrote {path}")


def first_error(payload: dict) -> str | None:
    err = payload.get("errors")
    if isinstance(err, dict) and err:
        k = next(iter(err.keys()))
        return f"{k}: {err.get(k)}"
    return None


def fetch_driver_ids_from_rankings(rankings_payload: dict) -> list[int]:
    """
    rankings/drivers response rows usually look like:
    { "position": 1, "driver": { "id": 123, ... }, ... }
    """
    ids = set()
    for row in (rankings_payload.get("response") or []):
        if not isinstance(row, dict):
            continue
        d = row.get("driver")
        if isinstance(d, dict) and d.get("id"):
            ids.add(d["id"])
    return sorted(ids)


def fetch_drivers_by_ids(driver_ids: list[int]) -> dict:
    """
    drivers endpoint requires at least one parameter.
    We'll call drivers?id=<id> for each driver id and combine responses.
    """
    out = {
        "get": "drivers",
        "parameters": {"ids": driver_ids, "season_source": YEAR},
        "errors": {},
        "results": 0,
        "response": [],
    }

    for did in driver_ids:
        payload = fetch_json("drivers", {"id": did})
        err = first_error(payload)
        if err:
            print(f"⚠️ drivers?id={did} returned error: {err}")
            continue

        resp = payload.get("response") or []
        if isinstance(resp, list):
            out["response"].extend(resp)

    out["results"] = len(out["response"])
    return out


def main() -> None:
    ensure_dir(OUT_DIR)

    # OPTION A: single source of truth ONLY in data/f1/<YEAR>/
    #
    # Confirmed behaviour (from your testing):
    # - teams supports season ✅
    # - circuits supports season ✅
    # - drivers requires at least one parameter (season alone fails) ❌
    #   => build drivers.json via standings driver ids

    jobs = [
        # seasons list (we still store it under YEAR folder for Option A)
        ("seasons", {}, OUT_DIR / "seasons.json"),

        # season-specific
        ("races", {"season": YEAR}, OUT_DIR / "races.json"),
        ("rankings/drivers", {"season": YEAR}, OUT_DIR / "standings_drivers.json"),
        ("rankings/teams", {"season": YEAR}, OUT_DIR / "standings_teams.json"),

        # season-scoped per your confirmation
        ("circuits", {"season": YEAR}, OUT_DIR / "circuits.json"),
        ("teams", {"season": YEAR}, OUT_DIR / "teams.json"),
    ]

    # Fetch & write core endpoints first
    for get_name, params, out_path in jobs:
        payload = fetch_json(get_name, params)
        err = first_error(payload)
        if err:
            print(f"⚠️ API returned error for {get_name} params={params}: {err}")
        write_json(out_path, payload)

    # Build drivers.json via ids from standings_drivers.json
    try:
        rankings_payload = json.loads((OUT_DIR / "standings_drivers.json").read_text(encoding="utf-8"))
    except Exception as e:
        print(f"⚠️ Could not read standings_drivers.json to derive driver ids: {e}")
        rankings_payload = {}

    driver_ids = fetch_driver_ids_from_rankings(rankings_payload)

    if not driver_ids:
        print("⚠️ No driver ids found in standings_drivers.json; writing empty drivers.json with error note")
        write_json(
            OUT_DIR / "drivers.json",
            {
                "get": "drivers",
                "parameters": {},
                "errors": {"ids": "No ids found from rankings/drivers"},
                "results": 0,
                "response": [],
            },
        )
    else:
        drivers_payload = fetch_drivers_by_ids(driver_ids)
        write_json(OUT_DIR / "drivers.json", drivers_payload)

    # Optional: race_results per race id
    if ENABLE_RACE_RESULTS:
        try:
            races_payload = json.loads((OUT_DIR / "races.json").read_text(encoding="utf-8"))
        except Exception as e:
            print(f"⚠️ Could not read races.json for race_results: {e}")
            return

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
