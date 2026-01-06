#!/usr/bin/env python3
import json
import os
import sys
import time
from pathlib import Path
from typing import Any, Dict, Optional
from urllib.parse import urlencode

import urllib.request
import urllib.error


BASE_URL_DEFAULT = "https://v1.formula-1.api-sports.io"


def build_headers() -> Dict[str, str]:
    """
    Supports both API-Sports direct key and RapidAPI, but prefers API-Sports direct key.
    """
    headers = {"Accept": "application/json"}

    apisports_key = os.getenv("APISPORTS_KEY", "").strip()
    if apisports_key:
        headers["x-apisports-key"] = apisports_key

    rapid_key = os.getenv("RAPIDAPI_KEY", "").strip()
    rapid_host = os.getenv("RAPIDAPI_HOST", "").strip()
    if rapid_key and rapid_host:
        headers["x-rapidapi-key"] = rapid_key
        headers["x-rapidapi-host"] = rapid_host

    return headers


def http_get_json(url: str, headers: Dict[str, str], retries: int = 3, backoff: float = 1.5) -> Dict[str, Any]:
    last_err: Optional[Exception] = None

    for attempt in range(1, retries + 1):
        try:
            req = urllib.request.Request(url, headers=headers, method="GET")
            with urllib.request.urlopen(req, timeout=30) as resp:
                raw = resp.read()
                return json.loads(raw.decode("utf-8"))
        except (urllib.error.HTTPError, urllib.error.URLError, TimeoutError, json.JSONDecodeError) as e:
            last_err = e
            if attempt < retries:
                time.sleep(backoff ** attempt)
            else:
                break

    raise RuntimeError(f"GET failed after {retries} attempts: {url}\nLast error: {last_err}")


def write_json(path: Path, payload: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    # stable formatting = cleaner git diffs
    path.write_text(json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8")


def endpoint(base_url: str, route: str, params: Optional[Dict[str, Any]] = None) -> str:
    if params:
        return f"{base_url.rstrip('/')}/{route.lstrip('/')}?{urlencode(params)}"
    return f"{base_url.rstrip('/')}/{route.lstrip('/')}"


def main() -> int:
    season = os.getenv("F1_SEASON", "2024").strip()
    base_url = os.getenv("F1_BASE_URL", BASE_URL_DEFAULT).strip()
    out_root = Path(os.getenv("F1_OUT_DIR", f"data/f1/{season}"))

    headers = build_headers()
    if "x-apisports-key" not in headers and "x-rapidapi-key" not in headers:
        print("❌ Missing API key. Set APISPORTS_KEY (recommended) or RAPIDAPI_KEY+RAPIDAPI_HOST.", file=sys.stderr)
        return 2

    # --- Core pulls (2024 folder) ---
    pulls = [
        ("season.json", "seasons", None),  # some APIs support ?season=YYYY; keep none unless needed
        ("races.json", "races", {"season": season, "type": "Race"}),  # confirmed param usage :contentReference[oaicite:3]{index=3}
        ("circuits.json", "circuits", {"season": season}),
        ("teams.json", "teams", {"season": season}),
        ("drivers.json", "drivers", {"season": season}),
        ("standings_drivers.json", "rankings/drivers", {"season": season}),
        ("standings_teams.json", "rankings/teams", {"season": season}),
    ]

    for filename, route, params in pulls:
        url = endpoint(base_url, route, params)
        print(f"→ {filename}: {url}")
        data = http_get_json(url, headers=headers)
        write_json(out_root / filename, data)

    # --- Race results per race id ---
    races_path = out_root / "races.json"
    races = json.loads(races_path.read_text(encoding="utf-8"))
    race_items = races.get("response") or []
    race_ids = [r.get("id") for r in race_items if isinstance(r, dict) and r.get("id") is not None]

    results_dir = out_root / "race_results"
    for race_id in race_ids:
        url = endpoint(base_url, "rankings/races", {"race": race_id})  # confirmed route exists :contentReference[oaicite:4]{index=4}
        print(f"→ race_results/{race_id}.json: {url}")
        data = http_get_json(url, headers=headers)
        write_json(results_dir / f"{race_id}.json", data)

    print(f"✅ Done. Wrote season {season} to: {out_root}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
