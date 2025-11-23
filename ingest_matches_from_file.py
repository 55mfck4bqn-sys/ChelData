#!/usr/bin/env python3
"""
ingest_matches_from_file.py

Reads matches JSON from a local file (e.g. matches.json) and writes them
into the Supabase `matches` table.

Usage:
  export SUPABASE_URL=...
  export SUPABASE_SERVICE_ROLE_KEY=...
  export CLUB_ID=26863

  python ingest_matches_from_file.py matches.json
"""

import os
import sys
import json
import logging
from datetime import datetime
from typing import Any, Dict, List

from supabase import create_client, Client

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s"
)
log = logging.getLogger("ingest_matches")

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_SERVICE_ROLE_KEY")
CLUB_ID = int(os.getenv("CLUB_ID", "26863"))

if not SUPABASE_URL or not SUPABASE_KEY:
    raise SystemExit("Missing SUPABASE_URL or SUPABASE_SERVICE_ROLE_KEY in env")

supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)


def load_json(path: str) -> Dict[str, Any] | List[Any]:
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def extract_matches(payload: Any) -> List[Dict[str, Any]]:
    """
    Try to normalize the JSON into a list of match dicts.
    Handles a few common shapes: {matches: [...]}, {data: [...]}, or just [...]
    """
    if isinstance(payload, dict):
        if "matches" in payload and isinstance(payload["matches"], list):
            return payload["matches"]
        if "data" in payload and isinstance(payload["data"], list):
            return payload["data"]
        # some APIs just put the list at top-level under some other key
        for v in payload.values():
            if isinstance(v, list) and v and isinstance(v[0], dict):
                return v
        # maybe it's actually a single match object
        if payload:
            return [payload]
        return []

    if isinstance(payload, list):
        # assume it's already a list of matches
        return payload

    return []


def upsert_matches(club_id: int, matches: List[Dict[str, Any]]):
    if not matches:
        log.info("No matches found in JSON payload.")
        return

    count = 0
    for m in matches:
        match_id = m.get("matchId") or m.get("id") or m.get("gameId")
        if match_id is None:
            # fallback unique-ish ID: timestamp ms + loop index
            match_id = int(datetime.utcnow().timestamp() * 1000) + count

        played_raw = m.get("playedAt") or m.get("startTime") or m.get("date")
        played_at = None
        if isinstance(played_raw, (int, float)):
            try:
                played_at = datetime.utcfromtimestamp(played_raw / 1000).isoformat()
            except Exception:
                played_at = None
        elif played_raw:
            played_at = str(played_raw)

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
        err = getattr(res, "error", None)
        if err:
            log.error("Supabase error upserting match %s: %s", match_id, err)
        else:
            count += 1

    log.info("Upserted %d matches into Supabase.", count)


def main():
    if len(sys.argv) < 2:
        print("Usage: python ingest_matches_from_file.py matches.json")
        raise SystemExit(1)

    path = sys.argv[1]
    if not os.path.exists(path):
        print(f"File not found: {path}")
        raise SystemExit(1)

    log.info("Loading matches from %s", path)
    payload = load_json(path)
    matches = extract_matches(payload)
    log.info("Found %d matches in JSON", len(matches))
    upsert_matches(CLUB_ID, matches)


if __name__ == "__main__":
    main()
