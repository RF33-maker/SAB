from supabase import create_client, Client
from typing import Optional, List, Dict
import os
import asyncio


SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")

if not SUPABASE_URL or not SUPABASE_KEY:
    raise ValueError("❌ Missing Supabase environment variables.")

supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

def fetch_player_records(player_name: str, league_id: Optional[str] = None, limit: int = 5) -> List[Dict]:
    try:
        # Assuming normalize_player_name is available in app.utils.voiceflow_tools
        # If not, you'll need to implement it or import it from where it's defined.
        # For demonstration, let's assume a simple implementation here if not imported.
        def normalize_player_name(name: str) -> str:
            import re
            return re.sub(r'\s*\(.*\)\s*', '', name).strip()

        # Normalize the search name to remove brackets
        normalized_name = normalize_player_name(player_name)

        # First try exact match (case-insensitive)
        query = supabase.table("player_stats").select("*").ilike("name", normalized_name)

        if league_id:
            print(f"🎯 Adding league_id filter: {league_id}")
            query = query.eq("league_id", league_id)

        response = query.order("game_date", desc=True).limit(limit).execute()

        if response.data:
            print(f"✅ Supabase returned {len(response.data)} records for '{player_name}' using pattern '{normalized_name}' with league_id '{league_id}'")
            return response.data
        else:
            print(f"🔍 No records found for pattern '{normalized_name}' with league_id '{league_id}'")

        # If no exact match, try wildcard search with normalized name
        wildcard_pattern = f"%{normalized_name}%"
        query = supabase.table("player_stats").select("*").ilike("name", wildcard_pattern)

        if league_id:
            print(f"🎯 Adding league_id filter: {league_id}")
            query = query.eq("league_id", league_id)

        response = query.order("game_date", desc=True).limit(limit).execute()

        if response.data:
            print(f"✅ Supabase returned {len(response.data)} records for '{player_name}' using pattern '{wildcard_pattern}' with league_id '{league_id}'")
            return response.data
        else:
            print(f"🔍 No records found for pattern '{wildcard_pattern}' with league_id '{league_id}'")

        # If still no match, try searching for the name with common captain designations
        captain_patterns = [f"{normalized_name} (C)", f"{normalized_name} (Captain)", f"{normalized_name}(C)"]

        for pattern in captain_patterns:
            query = supabase.table("player_stats").select("*").ilike("name", pattern)
            if league_id:
                query = query.eq("league_id", league_id)
            response = query.order("game_date", desc=True).limit(limit).execute()

            if response.data:
                print(f"✅ Found {len(response.data)} records using captain pattern '{pattern}' with league_id '{league_id}'")
                return response.data

        # Final wildcard attempt with captain patterns
        for pattern in captain_patterns:
            wildcard = f"%{pattern}%"
            query = supabase.table("player_stats").select("*").ilike("name", wildcard)
            if league_id:
                query = query.eq("league_id", league_id)
            response = query.order("game_date", desc=True).limit(limit).execute()

            if response.data:
                print(f"✅ Found {len(response.data)} records using captain wildcard '{wildcard}' with league_id '{league_id}'")
                return response.data

        return []

    except Exception as e:
        print(f"❌ Error fetching player records: {e}")
        return []