from supabase import create_client, Client
from typing import Optional
import os
import asyncio


SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")

if not SUPABASE_URL or not SUPABASE_KEY:
    raise ValueError("❌ Missing Supabase environment variables.")

supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

def fetch_player_records(player_name: Optional[str], league_id: Optional[str] = None):
    if not player_name:
        return []

    # Try exact match first, then fuzzy match
    queries_to_try = [
        f"{player_name}",  # Exact match
        f"%{player_name}%",  # Contains match
        f"{player_name}%",  # Starts with match
    ]
    
    for search_pattern in queries_to_try:
        query = supabase.table("player_stats").select("*").ilike("name", search_pattern)

        # Add league_id filter if provided
        if league_id:
            print(f"🎯 Adding league_id filter: {league_id}")
            query = query.eq("league_id", league_id)
        else:
            print("⚠️ No league_id provided for filtering")

        response = query.order("game_date", desc=True).limit(5).execute()

        if getattr(response, "error", None):
            print(f"❌ Supabase fetch error: {response.error}")
            continue

        if response.data:
            print(f"✅ Supabase returned {len(response.data)} records for '{player_name}' using pattern '{search_pattern}' with league_id '{league_id}'")
            return response.data
        else:
            print(f"🔍 No records found for pattern '{search_pattern}' with league_id '{league_id}'")
    
    print(f"❌ No records found for '{player_name}' in any search pattern")
    return []