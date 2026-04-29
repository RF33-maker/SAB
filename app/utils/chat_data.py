from supabase import create_client, Client
from typing import Optional, List, Dict
import os
import re
import logging


SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")

if not SUPABASE_URL or not SUPABASE_KEY:
    raise ValueError("Missing Supabase environment variables.")

supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

log = logging.getLogger("chat_data")


def _normalize_player_name(name: str) -> str:
    return re.sub(r'\s*\(.*\)\s*', '', name).strip()


def fetch_player_records(player_name: str, league_id: Optional[str] = None, limit: int = 5) -> List[Dict]:
    try:
        normalized_name = _normalize_player_name(player_name)
        wildcard_pattern = f"%{normalized_name}%"

        query = supabase.table("player_stats").select("*").ilike("full_name", wildcard_pattern)
        if league_id:
            query = query.eq("league_id", league_id)

        response = query.order("game_date", desc=True).limit(limit).execute()

        if response.data:
            log.info("Found %d records for '%s' (pattern='%s', league_id=%s)",
                     len(response.data), player_name, wildcard_pattern, league_id)
            return response.data

        log.debug("No records for pattern '%s' (league_id=%s)", wildcard_pattern, league_id)

        # Fallback: captain designation variants
        captain_patterns = [
            f"{normalized_name} (C)",
            f"{normalized_name} (Captain)",
            f"{normalized_name}(C)",
        ]

        for pattern in captain_patterns:
            q = supabase.table("player_stats").select("*").ilike("full_name", pattern)
            if league_id:
                q = q.eq("league_id", league_id)
            resp = q.order("game_date", desc=True).limit(limit).execute()
            if resp.data:
                log.info("Found %d records via captain pattern '%s'", len(resp.data), pattern)
                return resp.data

        for pattern in captain_patterns:
            q = supabase.table("player_stats").select("*").ilike("full_name", f"%{pattern}%")
            if league_id:
                q = q.eq("league_id", league_id)
            resp = q.order("game_date", desc=True).limit(limit).execute()
            if resp.data:
                log.info("Found %d records via captain wildcard '%s'", len(resp.data), pattern)
                return resp.data

        return []

    except Exception as e:
        log.error("Error fetching player records: %s", e, exc_info=True)
        return []
