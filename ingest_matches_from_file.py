#!/usr/bin/env python3

"""
ingest_matches_and_players.py

Reads an EA matches JSON file and ingests **both**:

  • match-level stats   → Supabase table: matches
  • player-level stats  → Supabase table: players

Requires a .env file in the same folder containing:

  SUPABASE_URL=https://<project>.supabase.co
  SUPABASE_SERVICE_ROLE_KEY=your_service_role_key
  CLUB_ID=26863
  PLATFORM=common-gen5
"""

import os
import sys
import json
import logging
from datetime import datetime
from typing import List, Dict, Any

from supabase import create_client, Client
from dotenv import load_dotenv

# ----------------------------
# Logging
# ----------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)
log = logging.getLogger("ingest")

# ----------------------------
# Load environment
# ----------------------------
load_dotenv()

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_SERVICE_ROLE_KEY")
CLUB_ID = int(os.getenv("CLUB_ID", "26863"))

if not SUPABASE_URL or not SUPABASE_KEY:
    raise SystemExit("Missing SUPABASE_URL or SUPABASE_SERVICE_ROLE_KEY in .env")

supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)
club_id_str = str(CLUB_ID)


# ----------------------------
# JSON LOADER
# ----------------------------
def load_json(path: str) -> Any:
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


# ----------------------------
# MATCH EXTRACTION
# ----------------------------
def extract_match_row(m: dict, club_id: int) -> dict:
    """Normalize and flatten match-level stats."""

    match_id = (
        m.get("matchId")
        or m.get("id")
        or m.get("gameId")
    )

    if match_id is None:
        match_id = int(datetime.utcnow().timestamp() * 1000)

    # Defaults
    goals_for = m.get("goalsFor") or m.get("teamScore")
    goals_against = m.get("goalsAgainst") or m.get("opponentScore")
    opponent_club = m.get("opponentClubId") or m.get("awayClubId")

    # Nested clubs block (preferred)
    clubs = m.get("clubs", {})
    if isinstance(clubs, dict) and club_id_str in clubs:
        your_club = clubs.get(club_id_str, {})

        opp_ids = [cid for cid in clubs.keys() if cid != club_id_str]
        opp_id = opp_ids[0] if opp_ids else None
        opp_block = clubs.get(opp_id, {})

        if isinstance(your_club, dict):
            goals_for = (
                your_club.get("goals") or
                your_club.get("score") or
                goals_for
            )

        if isinstance(opp_block, dict):
            goals_against = (
                opp_block.get("goals") or
                opp_block.get("score") or
                goals_against
            )

        if opp_id and opp_id.isdigit():
            opponent_club = int(opp_id)

    # Parse timestamp
    played_raw = (
        m.get("playedAt")
        or m.get("startTime")
        or m.get("date")
    )

    played_at = None
    if isinstance(played_raw, (int, float)):
        try:
            played_at = datetime.utcfromtimestamp(played_raw / 1000).isoformat()
        except:
            played_at = None
    elif played_raw:
        played_at = str(played_raw)

    return {
        "match_id": match_id,
        "club_id": club_id,
        "opponent_club_id": opponent_club,
        "goals_for": int(goals_for) if goals_for is not None else None,
        "goals_against": int(goals_against) if goals_against is not None else None,
        "result": m.get("result") or m.get("matchResult"),
        "match_type": m.get("matchType"),
        "played_at": played_at,
    }


def upsert_match(row: dict):
    res = supabase.table("matches").upsert(
        row,
        on_conflict="match_id"
    ).execute()

    if getattr(res, "error", None):
        log.error("ERROR upserting match %s: %s", row["match_id"], res.error)


# ----------------------------
# PLAYER EXTRACTION (CORRECT VERSION)
# ----------------------------
def extract_players_from_match(match: dict, club_id: int) -> List[dict]:
    """
    Extract per-player stats including:
    - playername
    - goals, assists, points, score
    - position
    """

    results = []
    club_id_str = str(club_id)

    # The stats live in: match["players"][clubId][playerId]
    stats_block = match.get("players", {}).get(club_id_str, {})

    for player_id_str, pdata in stats_block.items():
        try:
            player_id = int(player_id_str)
        except ValueError:
            continue

        goals = int(pdata.get("skgoals", 0))
        assists = int(pdata.get("skassists", 0))
        score = int(pdata.get("score", 0))
        points = goals + assists
        position = pdata.get("position")

        # ✔ Correct player name location
        name = pdata.get("playername")

        results.append({
            "match_id": match.get("matchId"),
            "club_id": club_id,
            "player_id": player_id,
            "player_name": name,
            "position": position,
            "goals": goals,
            "assists": assists,
            "points": points,
            "score": score,
            "result": match.get("result"),
        })

    return results


def upsert_players(players: List[dict]):
    for p in players:
        res = supabase.table("players").upsert(
            p,
            on_conflict="match_id,player_id"
        ).execute()

        if getattr(res, "error", None):
            log.error(
                "ERROR upserting player %s for match %s: %s",
                p["player_id"],
                p["match_id"],
                res.error,
            )


# ----------------------------
# MAIN
# ----------------------------
def main():
    if len(sys.argv) < 2:
        print("Usage: python3 ingest_matches_and_players.py matches.json")
        raise SystemExit(1)

    path = sys.argv[1]
    if not os.path.exists(path):
        raise SystemExit(f"File not found: {path}")

    log.info("Loading %s", path)
    payload = load_json(path)

    # If matches are under a nested key
    if isinstance(payload, dict):
        for key in ("matches", "data"):
            if key in payload and isinstance(payload[key], list):
                payload = payload[key]
                break

    if not isinstance(payload, list):
        raise SystemExit("JSON does not contain match list")

    log.info("Found %d matches", len(payload))

    for m in payload:
        match_row = extract_match_row(m, CLUB_ID)
        upsert_match(match_row)

        players = extract_players_from_match(m, CLUB_ID)
        upsert_players(players)

    log.info("Finished ingesting matches + players.")


if __name__ == "__main__":
    main()
