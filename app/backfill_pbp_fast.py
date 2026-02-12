#!/usr/bin/env python3
"""
Fast backfill script - processes multiple games without player/team lookups.
Populates live_events with core play-by-play data quickly.
Team/Player IDs can be populated later if needed.
"""

import requests
from app.utils.json_parser import supabase


def backfill_fast():
    """
    Fast backfill that skips team/player ID lookups for speed.
    """
    print("🔄 Starting FAST play-by-play backfill...")
    
    # Fetch all games with LiveStats URLs
    result = supabase.table("game_schedule").select(
        'game_key, league_id, "LiveStats URL"'
    ).execute()
    
    games = [g for g in result.data if g.get("LiveStats URL")]
    print(f"📊 Found {len(games)} games to process")
    
    processed = 0
    skipped = 0
    errors = 0
    
    for idx, game in enumerate(games, 1):
        game_key = game["game_key"]
        league_id = game["league_id"]
        livestats_url = game["LiveStats URL"]
        
        print(f"\n[{idx}/{len(games)}] 🎯 Processing {game_key}...")
        
        try:
            # Extract numeric ID from URL
            numeric_id = livestats_url.rstrip("/").split("/")[-1]
            data_json_url = f"https://fibalivestats.dcd.shared.geniussports.com/data/{numeric_id}/data.json"
            
            # Fetch the JSON data
            response = requests.get(data_json_url, timeout=10, headers={"User-Agent": "Mozilla/5.0"})
            
            if response.status_code != 200:
                print(f"   ⏭️  Skipped (HTTP {response.status_code})")
                skipped += 1
                continue
            
            data = response.json()
            
            # Extract play-by-play
            pbp = data.get("pbp", [])
            if not pbp:
                print(f"   ⏭️  No pbp data")
                skipped += 1
                continue
            
            # Get teams for name lookups only
            teams = data.get("tm", {})
            
            # Process events (without DB lookups)
            pbp_records = []
            for e in pbp:
                tno = e.get("tno")
                
                # Build score
                s1 = e.get("s1", "")
                s2 = e.get("s2", "")
                score = f"{s1}-{s2}" if s1 and s2 else None
                
                # Get qualifiers
                qualifiers = e.get("qualifier", [])
                
                pbp_record = {
                    "league_id": league_id,
                    "game_key": game_key,
                    "team_id": None,  # Skip for speed
                    "player_id": None,  # Skip for speed
                    "action_number": e.get("actionNumber"),
                    "period": e.get("period"),
                    "clock": e.get("clock"),
                    "player_name": e.get("player"),
                    "team_no": tno,
                    "action_type": e.get("actionType"),
                    "sub_type": e.get("subType"),
                    "qualifiers": qualifiers if qualifiers else None,
                    "success": e.get("success"),
                    "scoring": e.get("scoring"),
                    "points": None,
                    "score": score,
                    "x_coord": None,
                    "y_coord": None,
                    "description": None,
                }
                pbp_records.append(pbp_record)
            
            if pbp_records:
                try:
                    # Delete existing events for this game
                    supabase.table("live_events").delete().eq("game_key", game_key).execute()
                    # Insert new events
                    supabase.table("live_events").insert(pbp_records).execute()
                    print(f"   ✅ Inserted {len(pbp_records)} events")
                    processed += 1
                except Exception as e:
                    print(f"   ❌ Insert error: {e}")
                    errors += 1
            
        except Exception as e:
            print(f"   ❌ Error: {e}")
            errors += 1
    
    print(f"\n{'='*70}")
    print(f"✅ Backfill complete!")
    print(f"   Processed: {processed} games")
    print(f"   Skipped: {skipped} games")
    print(f"   Errors: {errors} games")
    print(f"{'='*70}")


if __name__ == "__main__":
    backfill_fast()
