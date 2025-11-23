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

            # Success path
            if resp.status_code == 200:
                try:
                    data = resp.json()
                except ValueError:
                    log.warning("Got HTTP 200 but body was not JSON")
                    data = {}

                if data == {}:
                    log.warning("[%d/%d] Empty JSON returned — retrying…",
                                attempt, max_retries)
                else:
                    return data

            # Retryable HTTP codes
            elif resp.status_code in (408, 429, 500, 502, 503, 504):
                log.warning(
                    "[%d/%d] HTTP %d from %s — will retry",
                    attempt, max_retries, resp.status_code, url
                )
            else:
                # Non-retryable error
                log.error(
                    "HTTP %d from %s — response: %s",
                    resp.status_code, url, resp.text[:300]
                )
                return {}

        except requests.RequestException as e:
            log.warning("[%d/%d] Exception calling %s: %s",
                        attempt, max_retries, url, str(e))

        # Backoff before next attempt
        sleep_seconds = base_backoff * attempt
        log.info("Sleeping for %ds before retry to %s", sleep_seconds, url)
        time.sleep(sleep_seconds)

    log.error("Max retries reached for %s", url)
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
# Upsert functions
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
        log.warning("Empty stats JSON; skipping club_stats insert")
        return

    data = stats_json.get("data") or stats_json.get("clubs") or stats_json
    if isinstance(data, list):
        data = data[0] if data else {}

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
        match_id = m.get("matchId") or m.get("id") or m.get("gameId") or int(time.time() * 1000)

        played_raw = m.get("playedAt") or m.get("startTime") or m.get("date")
        if isinstance(played_raw, (int, float)):
            played_at = datetime.utcfromtimestamp(played_raw / 1000).isoformat()
        else:
            played_at = str(played_raw) if played_raw else None

        row = {
            "match_id": match_id,
            "club_id": club_id,
            "opponent_club_id": m.get("opponentClubId") or m.get("awayClubId"),
            "goals_for": m.get("goalsFor") or m.get("teamScore"),
            "goals_against": m.get("goalsAgainst") or m.get("opponentScore"),
            "result": m.get("result") or m.get("matchResult"),
            "match_type": m.get("matchType"),
            "played_at": played_at,
        }

        res = supabase.table("matches").upsert(row, on_conflict="match_id").execute()
        if check_supabase(res, "upsert_match"):
            count += 1

    log.info("Processed %d matches", count)

def upsert_members_and_stats(club_id: int, members_json: Dict[str, Any]):
    members = members_json.get("members") or members_json.get("data") or []
    if not members:
        log.info("No members returned from EA")
        return

    count = 0
    for m in members:
        member_id = m.get("memberId") or m.get("playerId") or m.get("id")
        if not member_id:
            continue

        member_row = {
            "member_id": member_id,
            "club_id": club_id,
            "name": m.get("displayName") or m.get("name") or m.get("playerName"),
            "position": m.get("position") or m.get("preferredPosition"),
        }

        res = supabase.table("members").upsert(member_row, on_conflict="member_id").execute()
        check_supabase(res, "upsert_member")

        stats = m.get("stats") or m
        stats_row = {
            "member_id": member_id,
            "club_id": club_id,
            "games_played": stats.get("gamesPlayed"),
            "goals": stats.get("goals"),
            "assists": stats.get("assists"),
            "plus_minus": stats.get("plusMinus") or stats.get("plus_minus"),
            "toi": stats.get("timeOnIce") or stats.get("toi"),
            "timestamp": datetime.utcnow().isoformat(),
        }

        res2 = supabase.table("member_stats").insert(stats_row).execute()
        check_supabase(res2, "insert_member_stats")

        count += 1

    log.info("Processed %d members", count)

# -------------------
# Main entrypoint
# -------------------
def main():
    log.info("Starting fetch for club %s on platform %s", CLUB_ID, PLATFORM)

    # 1) Club stats
    stats_json = fetch_club_stats(CLUB_ID, PLATFORM)

    # Extract club name if possible
    club_name = None
    if isinstance(stats_json, dict):
        if "clubs" in stats_json and stats_json["clubs"]:
            club_name = stats_json["clubs"][0].get("clubName")
        elif "data" in stats_json and stats_json["data"]:
            club_name = stats_json["data"][0].get("clubName") or stats_json["data"][0].get("clubNameText")

    if not club_name:
        club_name = f"club_{CLUB_ID}"

    upsert_club(CLUB_ID, club_name, PLATFORM)
    upsert_club_stats(CLUB_ID, stats_json)

    # 2) Matches
    matches_json = fetch_match_history(CLUB_ID, PLATFORM)
    upsert_matches(CLUB_ID, matches_json)

    # 3) Members
    members_json = fetch_member_stats(CLUB_ID, PLATFORM)
    upsert_members_and_stats(CLUB_ID, members_json)

    log.info("Finished scrape for club %s", CLUB_ID)

if __name__ == "__main__":
    main()
