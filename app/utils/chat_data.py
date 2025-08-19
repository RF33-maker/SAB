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
        print(f"🔍 Searching for player: '{player_name}' in league: {league_id}")
        
        # First try exact match (including position indicators)
        exact_query = supabase.table("player_stats").select("*").eq("name", player_name)

        if league_id:
            exact_query = exact_query.eq("league_id", league_id)

        exact_response = exact_query.order("game_date", desc=True).limit(limit).execute()

        if exact_response.data:
            print(f"✅ Found exact match for '{player_name}': {len(exact_response.data)} records")
            # Verify we got the right player by checking the first record
            first_record = exact_response.data[0]
            print(f"✅ Confirmed player: {first_record.get('name')} from team {first_record.get('team')}")
            return exact_response.data

        # If no exact match, try base name matching (without position indicators)
        base_name = player_name.split('(')[0].strip() if '(' in player_name else player_name
        print(f"🔍 No exact match, trying base name: '{base_name}'")
        
        base_query = supabase.table("player_stats").select("*").ilike("name", f"{base_name}%")

        if league_id:
            base_query = base_query.eq("league_id", league_id)

        base_response = base_query.order("game_date", desc=True).limit(limit).execute()

        if base_response.data:
            # Filter to find exact base name matches
            exact_base_matches = [
                r for r in base_response.data 
                if r.get('name', '').split('(')[0].strip().lower() == base_name.lower()
            ]
            
            if exact_base_matches:
                print(f"✅ Found {len(exact_base_matches)} base name matches for '{base_name}'")
                first_record = exact_base_matches[0]
                print(f"✅ Confirmed player: {first_record.get('name')} from team {first_record.get('team')}")
                return exact_base_matches
            else:
                print(f"⚠️ Found fuzzy matches but no exact base name match for '{base_name}'")
                # Return fuzzy matches as fallback
                first_record = base_response.data[0]
                print(f"⚠️ Returning fuzzy match: {first_record.get('name')} from team {first_record.get('team')}")
                return base_response.data

        # Last resort: broader fuzzy search
        print(f"🔍 No base name match, trying fuzzy search for: '{player_name}'")
        fuzzy_query = supabase.table("player_stats").select("*").ilike("name", f"%{player_name}%")

        if league_id:
            fuzzy_query = fuzzy_query.eq("league_id", league_id)

        fuzzy_response = fuzzy_query.order("game_date", desc=True).limit(limit).execute()

        if fuzzy_response.data:
            print(f"✅ Found {len(fuzzy_response.data)} fuzzy matches for '{player_name}'")
            first_record = fuzzy_response.data[0]
            print(f"⚠️ Returning fuzzy match: {first_record.get('name')} from team {first_record.get('team')}")
            return fuzzy_response.data
        else:
            print(f"❌ No records found for '{player_name}'")
            return []

    except Exception as e:
        print(f"❌ Error fetching player records: {e}")
        return []