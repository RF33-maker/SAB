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

    query = supabase.table("player_stats").select("*").ilike("name", player_name)

    if game_id:
        query = query.eq("game_id", game_id)

    # Add league_id filter if provided
    if league_id:
        query = query.eq("league_id", league_id)

    response = query.order("game_date", desc=True).limit(5).execute()

    if getattr(response, "error", None):
        print(f"❌ Supabase fetch error: {response.error}")
        return []

    print(f"✅ Supabase returned {len(response.data)} records for '{player_name}'")
    return response.data