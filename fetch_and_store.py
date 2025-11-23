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
    tim
