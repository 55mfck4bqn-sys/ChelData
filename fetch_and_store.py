#!/usr/bin/env python3
"""
fetch_and_store.py
Fetches club stats, matches, and member stats for a Pro Clubs NHL club
and upserts them into Supabase tables.

Env vars required:
- SUPABASE_URL
- SUPABASE_SERVICE_ROLE_KEY
- CLUB_ID (optional, defaults to 26863)
- PLATFORM (optional, defaults to common-gen5)
"""

import os
import time
import logging
from datetime import datetime
from typing import Dict, Any

import requests
from supabase import create_client, Client

# -------------------
# Environment / config
# -------------------
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_SERVICE_ROLE_KEY")
CLUB_ID = int(os.getenv("CLUB_ID", "26863"))
PLATFORM = os.getenv("PLATFORM", "common-gen5")

if not SUPABASE_URL or not SUPABASE_KEY:
    raise SystemExit("Missing SUPABASE_URL or SUPABASE_SERVICE_ROLE_KEY in environment")

EA_BASE = "https://proclubs.ea.com/api/nhl"

HEADERS = {
    "Referer": "https://www.ea.com",
    "User-Agent": "nhl-proclubs-scraper/1.0"
}

# -------------------
# Logging
# -------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s"
)
log = logging.getLogger("nhl_scraper")

# -------------------
# Supabase client
# -------------------
supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

# -------------------
# Helper: check Supabase response
# -------------------
def check_supabase(res, context: str) -> bool:
    """
    Helper to log Supabase errors consistently.
    Supabase Python client returns an APIResponse with .data and .error.
    """
    err = getattr(res, "error", None)
    if err:
        log.error("Supabase error in %s: %s", context, err)
        return False
    else:
        log.debug("Supabase OK in %s, data: %s", context, getattr(res, "data", None))
        return True

# -------------------
# HTTP helper with retries
# -------------------
def get_json_with_retry(
    url: str,
    params: Dict[str, Any] | None = None,
    max_retries: int = 7,
    base_backoff: int = 5,
    timeout: int = 30,
) -> Dict[str, Any]:
    """
    Robust GET with retries, backoff, and handling for empty JSON / timeouts.
    """
    for attempt in range(1, max_retries + 1):
        try:
            log.info(
                "Requesting %s (attempt %d/%d, timeout=%ds)",
                url,
                attempt,
                max_retries,
                timeout,
            )
            resp = requests.get(
                url,
                params=params,
                headers=HEADERS,
                timeout=timeout,
            )

            status = resp.status_code

            # Happy path
            if status == 200:
                try:
                    data = resp.json()
                except ValueError:
                    log.warning("Got 200 but response was not JSON")
                    data = {}

                # EA sometimes returns {} when it's unhappy; treat that as retryable
                if data == {}:
                    log.warning(
                        "[%d/%d] 200 OK but empty JSON from %s – will retry",
                        attempt,
                        max_retries,
                        url,
                    )
                else:
                    return data

            # Transient HTTP errors we should retry
            elif status in (408, 429, 500, 502, 503, 504):
                log.warning(
                    "[%d/%d] HTTP %d from %s – will retry",
                    attempt,
                    max_retries,
                    status,
                    url,
                )
            else:
                # Non-retryable error – log and bail
                log.error(
                    "[%d/%d] HTTP %d from %s – response: %s",
                    attempt,
                    max_retries,
                    status,
                    url,
                    resp.text[:300],
                )
                return {}

        except requests.RequestException as e:
            log.warning(
                "[%d/%d] Exception when calling %s: %s",
                attempt,
                max_retries,
                url,
                str(e),
            )

        # Backoff before next attempt
        sleep_seconds = base_backoff * attempt
        log.info("Sleeping for %ds before retrying %s", sleep_seconds, url)
        time.sleep(sleep_seconds)

    log.error("Max retries reached for %s; returning empty dict", url)
    return {}

# -------------------
# EA API wrappers
# -------------------
def fetch_club_stats(club_id: int, platform: str) -> Dict[str, Any]:
    url = f"{EA_BASE}/clubs/stats"
    params = {"clubIds": club_id, "platform": platform}
    return get_json_with_retry(url, params=params)

def fetch_match_history(club_id: int, platform: str, match_type: str = "gameType5") -> Dict[str, Any]:
    url = f"{EA_BASE}/clubs/matches"
    params = {"clubIds": club_id, "platform": platform, "matchType": match_type}
    return get_json_with_retry(url, params=params)

def fetch_member_stats(club_id: int, platform: str) -> Dict[str, Any]:
    url = f"{EA_BASE}/members/stats"
    params = {"clubId": club_id, "platform": platform}
    return get_json_with_retry(url, params=params)

# -------------------
# Supabase upsert helpers
# -------------------
def upsert_club(club_id: int, club_name: str, platform: str):
    payload = {
        "club_id": club_id,
        "name": club_name,
        "platform": platform,
    }
    res = supabase.table("clubs").upsert(payload, on_conflict="club_id").execute()
    if check_supabase(res, "upsert_club"):
        log.info("Upserted club %s (%s)", club_id, club_name)

def upsert_club_stats(club_id: int, stats_json: Dict[str, Any]):
    if not stats_json:
        log.warning("No club stats JSON to write (empty payload)")
        return

    data = stats_json.get("data") or stats_json.get("clubs") or stats_json
    if isinstance(data, list):
        data = data[0] if data else {}

    if not isinstance(data, dict):
        log.warning("Unexpected club stats shape: %s", type(data))
        return

    record = {
        "club_id": club_id,
        "games_played": data.get("gamesPlayed"),
        "goals_for": data.get("goalsFor") or data.get("goalsScored"),
        "goals_against": data.get("goalsAgainst"),
        "wins": data.get("wins"),
        "losses": data.get("losses"),
        "ot_losses": data.get("otLosses") or data.get("overtimeLosses"),
        "pp_pct": data.get("powerPlayPercentage") or data.get("ppPercent"),
        "pk_pct": data.get("penaltyKillPercentage") or data.get("pkPercent"),
        "timestamp": datetime.utcnow().isoformat(),
    }

    res = supabase.table("club_stats").insert(record).execute()
    if check_supabase(res, "insert_club_stats"):
        log.info("Inserted club_stats snapshot for %s", club_id)

def upsert_matches(club_id: int, matches_json: Dict[str, Any]):
    matches = matches_json.get("matches") or matches_json.get("data") or []
    if not matches:
        log.info("No matches returned from EA")
        return

    count = 0
    for m in matches:
        match_id = m.get("matchId") or m.get("id") or m.get("gameId")
        if match_id is None:
            # fallback unique ID – not ideal but avoids null PK
            match_id = int(time.time() * 1000)

        played_at_raw = m.get("playedAt") or m.get("startTime") or m.get("date")
        played_at = None
        if played_at_raw:
            try:
                if isinstance(played_at_raw, (int, float)):
                    played_at = datetime.utcfromtimestamp(int(played_at_raw) / 1000).isoformat()
                else:
                    played_at = str(played_at_raw)
            except Exception:
                played_at = str(played_at_raw)

        row = {
