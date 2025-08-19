from supabase import create_client, Client
from typing import Optional
import os
import asyncio


SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")

if not SUPABASE_URL or not SUPABASE_KEY:
    raise ValueError("❌ Missing Supabase environment variables.")

supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

def fetch_player_records(player_name, league_id=None, limit=5):
    """
    Fetch player records from Supabase, ordered by most recent game first.
    Returns a list of dictionaries containing player stats.
    """
    try:
        # First try exact match
        exact_query = supabase.table("player_stats").select("*").eq("name", player_name)

        if league_id:
            exact_query = exact_query.eq("league_id", league_id)

        exact_response = exact_query.order("game_date", desc=True).limit(limit).execute()

        if exact_response.data:
            print(f"✅ Found exact match for '{player_name}': {len(exact_response.data)} records")
            return exact_response.data

        # If no exact match, try pattern matching
        query = supabase.table("player_stats").select("*").ilike("name", f"%{player_name}%")

        if league_id:
            query = query.eq("league_id", league_id)

        # Order by game_date descending to get most recent games first
        query = query.order("game_date", desc=True).limit(limit)

        response = query.execute()

        # Filter results to find the best match (prefer exact name without position)
        if response.data:
            # First, look for exact name match (ignoring position indicators)
            base_name = player_name.split('(')[0].strip() if '(' in player_name else player_name
            exact_matches = [r for r in response.data if r.get('name', '').split('(')[0].strip().lower() == base_name.lower()]

            if exact_matches:
                print(f"✅ Found {len(exact_matches)} exact name matches for '{player_name}'")
                return exact_matches

            print(f"✅ Supabase returned {len(response.data)} fuzzy matches for '{player_name}' using pattern '%{player_name}%'")
            return response.data
        else:
            print(f"❌ No records found for '{player_name}'")
            return []

    except Exception as e:
        print(f"❌ Error fetching player records: {e}")
        return []