#!/usr/bin/env python3
"""
fetch_and_store.py
Fetches club stats, matches, and member stats for club 26863 (common-gen5)
and upserts them into Supabase tables: clubs, club_stats, matches, members, member_stats.

Env vars required:
- SUPABASE_URL
- SUPABASE_SERVICE_ROLE_KEY
- CLUB_ID (optional, defaults to 26863)
- PLATFORM (optional, defaults to common-gen5)
"""

import os
import time
import logging
import requests
from typing import List, Dict, Any
from datetime import datetime
from supabase import create_client, Client

# --- Config / env ---
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_SERVICE_ROLE_KEY")
CLUB_ID = int(os.getenv("CLUB_ID", "26863"))
PLATFORM = os.getenv("PLATFORM", "common-gen5")

if not SUPABASE_URL or not SUPABASE_KEY:
    raise SystemExit("Missing SUPABASE_URL or SUPABASE_SERVICE_ROLE_KEY in environment")

# --- Setup ---
log = logging.getLogger("nhl_scraper")
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

# --- HTTP helpers with simple retry/backoff ---
HEADERS = {
    "Referer": "https://www.ea.com",
    "User-Agent": "nhl-proclubs-scraper/1.0 (+https://yourproject.example)"
}

def get_json_with_retry(url: str, params: Dict[str, Any] = None, max_retries: int = 5, backoff: float = 1.0):
    for attempt in range(1, max_retries + 1):
        try:
            resp = requests.get(url, params=params, headers=HEADERS, timeout=15)
            if resp.status_code == 200:
                try:
                    return resp.json()
                except ValueError:
                    log.warning("Non-JSON response from %s", url)
                    return {}
            elif resp.status_code in (429, 502, 503, 504):
                log.warning("Transient status %s from %s; attempt %d/%d", resp.status_code, url, attempt, max_retries)
            else:
                # For 403/401 the body may be {}
                log.info("Status %s from %s: %s", resp.status_code, url, resp.text[:200])
                # Return {} to allow caller to decide
                return {}
        except requests.RequestException as e:
            log.warning("Request error for %s: %s", url, str(e))
        # backoff
        time.sleep(backoff * attempt)
    log.error("Max retries reached for %s", url)
    return {}

# --- Fetch functions ---
def fetch_club_stats(club_id: int, platform: str) -> Dict[str, Any]:
    url = "https://proclubs.ea.com/api/nhl/clubs/stats"
    params = {"clubIds": club_id, "platform": platform}
    return get_json_with_retry(url, params=params)

def fetch_match_history(club_id: int, platform: str, match_type: str = "gameType5") -> Dict[str, Any]:
    url = "https://proclubs.ea.com/api/nhl/clubs/matches"
    params = {"clubIds": club_id, "platform": platform, "matchType": match_type}
    return get_json_with_retry(url, params=params)

def fetch_member_stats(club_id: int, platform: str) -> Dict[str, Any]:
    url = "https://proclubs.ea.com/api/nhl/members/stats"
    params = {"clubId": club_id, "platform": platform}
    return get_json_with_retry(url, params=params)

# --- Transform / Upsert helpers ---
def upsert_club(club_id: int, club_name: str, platform: str):
    payload = {
        "club_id": club_id,
        "name": club_name,
        "platform": platform
    }
    res = supabase.table("clubs").upsert(payload, on_conflict="club_id").execute()
    if res.status_code >= 400:
        log.error("Failed upsert clubs: %s", res.data)
    else:
        log.info("Upserted club %s", club_id)

def upsert_club_stats(club_id: int, stats_json: Dict[str, Any]):
    # Adjust keys depending on API structure. We'll attempt to parse common fields.
    data = stats_json.get("data") or stats_json.get("clubs") or stats_json
    if not data:
        log.warning("No club stats payload to write")
        return

    # If data is a list, use first element
    if isinstance(data, list):
        data = data[0] if data else {}

    # Map expected fields with safe access
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
        "timestamp": datetime.utcnow().isoformat()
    }

    res = supabase.table("club_stats").insert(record).execute()
    if res.status_code >= 400:
        log.error("Failed insert club_stats: %s", res.data)
    else:
        log.info("Inserted club_stats for %s", club_id)

def upsert_matches(club_id: int, matches_json: Dict[str, Any]):
    matches = matches_json.get("matches") or matches_json.get("data") or []
    if not matches:
        log.info("No matches to upsert")
        return

    rows = []
    for m in matches:
        # Attempt to collect relevant fields safely
        match_id = m.get("matchId") or m.get("id") or m.get("gameId")
        played_at_raw = m.get("playedAt") or m.get("startTime") or m.get("date")
        played_at = None
        if played_at_raw:
            try:
                # many EA timestamps are UNIX ms or iso strings — try both
                if isinstance(played_at_raw, (int, float)):
                    # assume ms
                    played_at = datetime.utcfromtimestamp(int(played_at_raw) / 1000).isoformat()
                else:
                    played_at = str(played_at_raw)
            except Exception:
                played_at = str(played_at_raw)

        rows.append({
            "match_id": match_id,
            "club_id": club_id,
            "opponent_club_id": m.get("opponentClubId") or m.get("awayClubId") or None,
            "goals_for": m.get("goalsFor") or m.get("teamScore") or None,
            "goals_against": m.get("goalsAgainst") or m.get("opponentScore") or None,
            "result": m.get("result") or m.get("matchResult"),
            "match_type": m.get("matchType"),
            "played_at": played_at
        })

    # Upsert by match_id to avoid duplicates
    # Supabase upsert requires on_conflict param; but the python client .upsert is available on newer versions.
    for r in rows:
        if r["match_id"] is None:
            # If there's no match_id, create a synthetic unique key by hashing
            r["match_id"] = int(time.time() * 1000)  # fallback — not ideal but prevents null PK
        res = supabase.table("matches").upsert(r, on_conflict="match_id").execute()
        if res.status_code >= 400:
            log.error("Failed upsert match %s: %s", r.get("match_id"), res.data)
        else:
            log.debug("Upserted match %s", r.get("match_id"))

    log.info("Upserted %d matches (attempted)", len(rows))

def upsert_members_and_stats(club_id: int, members_json: Dict[str, Any]):
    members = members_json.get("members") or members_json.get("data") or []
    if not members:
        log.info("No members to upsert")
        return

    for m in members:
        member_id = m.get("memberId") or m.get("playerId") or m.get("id")
        if not member_id:
            continue
        # upsert members table
        member_row = {
            "member_id": member_id,
            "club_id": club_id,
            "name": m.get("displayName") or m.get("name") or m.get("playerName"),
            "position": m.get("position") or m.get("preferredPosition")
        }
        res = supabase.table("members").upsert(member_row, on_conflict="member_id").execute()
        if res.status_code >= 400:
            log.error("Failed upsert member %s: %s", member_id, res.data)

        # upsert member_stats snapshot
        stats = m.get("stats") or m  # sometimes stats are nested; sometimes top-level
        member_stats_row = {
            "member_id": member_id,
            "club_id": club_id,
            "games_played": stats.get("gamesPlayed"),
            "goals": stats.get("goals"),
            "assists": stats.get("assists"),
            "plus_minus": stats.get("plusMinus") or stats.get("plus_minus"),
            "toi": stats.get("timeOnIce") or stats.get("toi"),
            "timestamp": datetime.utcnow().isoformat()
        }
        res2 = supabase.table("member_stats").insert(member_stats_row).execute()
        if res2.status_code >= 400:
            log.error("Failed insert member_stats for %s: %s", member_id, res2.data)

    log.info("Processed %d members", len(members))

# --- Main flow ---
def main():
    log.info("Starting fetch for club %s on platform %s", CLUB_ID, PLATFORM)

    # 1) Club search to get canonical club name (optional, idempotent)
    search_url = "https://proclubs.ea.com/api/nhl/clubs/search"
    q = {"platform": PLATFORM, "clubName": ""}
    # The search endpoint normally expects a name; we already have id, but some responses include info in stats
    # We'll try club stats first and fall back to search if needed.

    # 2) Club stats
    stats_json = fetch_club_stats(CLUB_ID, PLATFORM)
    # Attempt to discover club name
    club_name = None
    # Many endpoints return data->clubName or clubs->[0]->clubName
    if isinstance(stats_json, dict):
        # common shapes
        if "clubs" in stats_json and isinstance(stats_json["clubs"], list) and stats_json["clubs"]:
            club_name = stats_json["clubs"][0].get("clubName")
        if not club_name and "data" in stats_json and isinstance(stats_json["data"], list) and stats_json["data"]:
            club_name = stats_json["data"][0].get("clubName") or stats_json["data"][0].get("clubNameText")
        # fallbacks
        if not club_name:
            club_name = f"club_{CLUB_ID}"
    else:
        club_name = f"club_{CLUB_ID}"

    # Upsert basic club record
    upsert_club(CLUB_ID, club_name, PLATFORM)

    # Upsert club stats snapshot
    upsert_club_stats(CLUB_ID, stats_json)

    # 3) Matches
    matches_json = fetch_match_history(CLUB_ID, PLATFORM)
    upsert_matches(CLUB_ID, matches_json)

    # 4) Members / player stats
    members_json = fetch_member_stats(CLUB_ID, PLATFORM)
    upsert_members_and_stats(CLUB_ID, members_json)

    log.info("Finished scrape for club %s", CLUB_ID)

if __name__ == "__main__":
    main()
