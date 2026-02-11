
import time
import requests
from app.utils.json_parser import (
    supabase,
    normalize_team_name,
    normalize_player_name,
    get_or_create_team,
    get_or_create_player,
    insert_supabase,
    TEAM_FIELD_MAP,
    PLAYER_FIELD_MAP
)

POLL_INTERVAL = 10


def is_game_finished(data):
    """Detect if a game has finished based on the live data."""
    game_status = data.get("status", {})
    
    # Check explicit status field
    if game_status.get("finished") or game_status.get("final"):
        return True
    
    # Check if game has ended based on period
    current_period = game_status.get("period") or game_status.get("periodNumber")
    if current_period and int(current_period) >= 4:
        # Check if clock is expired (00:00 or empty)
        clock = game_status.get("clock", "")
        if not clock or clock == "00:00" or clock == "0:00":
            return True
    
    # Check plays for end of game indicator
    plays = data.get("Plays") or data.get("plays") or []
    if plays:
        last_play = plays[-1]
        # If last play is in period 4+ and it's an "end of game" type action
        if last_play.get("period", 0) >= 4 and last_play.get("actionType") in ["gameend", "endofgame"]:
            return True
    
    return False


def finalize_game_stats(game_key):
    """Mark all stats for a game as 'final' once the game is complete."""
    try:
        # Update player_stats
        supabase.table("player_stats").update({"status": "final"}).eq("game_key", game_key).execute()
        
        # Update team_stats
        supabase.table("team_stats").update({"status": "final"}).eq("game_key", game_key).execute()
        
        # Mark game as complete in game_schedule
        supabase.table("game_schedule").update({"status": "final"}).eq("game_key", game_key).execute()
        
        print(f"🏁 Game {game_key} marked as FINAL")
        return True
    except Exception as e:
        print(f"❌ Error finalizing game {game_key}: {e}")
        return False


def fetch_active_games():
    """Return all games from game_schedule that have a LiveStats URL and aren't final yet."""
    res = supabase.table("game_schedule").select(
        "game_key, league_id, home_team_id, away_team_id, competitionname, LiveStats URL, status"
    ).execute()
    
    return [
        g for g in res.data
        if g.get("LiveStats URL") 
        and "data.json" in g.get("LiveStats URL", "")
        and g.get("status") != "final"
    ]


def process_game(game):
    """Process a single live game, extracting all stats and maintaining relationships."""
    game_key = game["game_key"]
    league_id = game["league_id"]
    livestats_url = game["LiveStats URL"]
    
    try:
        data = requests.get(livestats_url, timeout=10).json()
    except Exception as e:
        print(f"❌ Error fetching {game_key}: {e}")
        return

    if not data:
        print(f"⚠️ No data for {game_key}")
        return

    teams = data.get("tm", {})
    plays = data.get("Plays") or data.get("plays") or []
    
    if not plays and not teams:
        print(f"⚠️ No plays or team data yet for {game_key}")
        return

    numeric_id = livestats_url.rstrip("/").split("/")[-1]

    try:
        # --- Process team stats ---
        team_records = []
        team_id_map = {}
        
        for side, team in teams.items():
            team_name = team.get("name")
            if not team_name:
                continue
                
            team_id = get_or_create_team(league_id, team_name)
            team_id_map[side] = team_id
            
            team_record = {
                "numeric_id": numeric_id,
                "side": side,
                "game_key": game_key,
                "team_id": team_id,
                "league_id": league_id,
                "identifier_duplicate": f"{numeric_id}_{team_id}_{side}",
                "status": "live"
            }
            for json_key, db_key in TEAM_FIELD_MAP.items():
                team_record[db_key] = team.get(json_key)
            team_records.append(team_record)

        if team_records:
            insert_supabase("team_stats", team_records, conflict_keys="identifier_duplicate")
            print(f"📊 {len(team_records)} team stats synced for {game_key}")

        # --- Process player stats ---
        player_records = []
        player_id_map = {}
        
        for side, team in teams.items():
            team_name = team.get("name")
            if not team_name:
                continue
                
            team_id = team_id_map.get(side)
            if not team_id:
                continue
            
            for pid, player in team.get("pl", {}).items():
                full_name = f"{player.get('firstName', '')} {player.get('familyName', '')}".strip()
                if not full_name:
                    continue
                    
                normalized_name = normalize_player_name(full_name)
                player_id = get_or_create_player(
                    normalized_name, 
                    team_id, 
                    player.get("shirtNumber"),
                    team_name,
                    league_id
                )
                
                # Key by (team_id, normalized_name) to prevent collisions between players with same name on different teams
                player_id_map[(team_id, normalized_name)] = {
                    "player_id": player_id,
                    "team_id": team_id
                }

                player_record = {
                    "numeric_id": numeric_id,
                    "side": side,
                    "game_key": game_key,
                    "team_id": team_id,
                    "player_id": player_id,
                    "full_name": normalized_name,
                    "team_name": team_name,
                    "league_id": league_id,
                    "identifier_duplicate": f"{numeric_id}_{player_id}",
                    "status": "live"
                }
                for json_key, db_key in PLAYER_FIELD_MAP.items():
                    player_record[db_key] = player.get(json_key)
                player_records.append(player_record)

        if player_records:
            insert_supabase("player_stats", player_records, conflict_keys="identifier_duplicate")
            print(f"👤 {len(player_records)} player stats synced for {game_key}")

        # --- Process live events (plays) ---
        new_plays, shots = [], []

        for p in plays:
            player_name = normalize_player_name(p.get("player", ""))
            team_no = p.get("tno")
            
            # Get team_id first, then lookup player by (team_id, name)
            team_id = team_id_map.get(team_no)
            player_info = player_id_map.get((team_id, player_name), {}) if team_id else {}
            player_id = player_info.get("player_id")

            play = {
                "game_key": game_key,
                "league_id": league_id,
                "team_id": team_id,
                "player_id": player_id,
                "action_number": p.get("actionNumber"),
                "period": p.get("period"),
                "clock": p.get("clock"),
                "player_name": player_name,
                "team_no": team_no,
                "action_type": p.get("actionType"),
                "sub_type": p.get("subType"),
                "qualifiers": p.get("qualifier", []),
                "success": bool(p.get("success")),
                "scoring": bool(p.get("scoring")),
                "score": f"{p.get('s1')}-{p.get('s2')}",
                "x_coord": p.get("x"),
                "y_coord": p.get("y"),
            }
            new_plays.append(play)

            if p.get("actionType") in ["2pt", "3pt", "freethrow"]:
                shots.append({
                    "game_key": game_key,
                    "league_id": league_id,
                    "team_id": team_id,
                    "player_id": player_id,
                    "player_name": player_name,
                    "team_no": team_no,
                    "period": p.get("period"),
                    "clock": p.get("clock"),
                    "shot_type": p.get("actionType"),
                    "sub_type": p.get("subType"),
                    "success": bool(p.get("success")),
                    "x": p.get("x"),
                    "y": p.get("y"),
                })

        if new_plays:
            # Use composite conflict key to prevent cross-game data corruption
            supabase.table("live_events").upsert(new_plays, on_conflict="game_key,action_number").execute()
            print(f"✅ {len(new_plays)} plays synced for {game_key}")

        if shots:
            supabase.table("shot_chart").upsert(
                shots,
                on_conflict=["game_key","player_name","period","clock"]
            ).execute()
            print(f"🎯 {len(shots)} shots synced for {game_key}")

        # Check if game is finished and finalize if so
        if is_game_finished(data):
            finalize_game_stats(game_key)

    except Exception as e:
        print(f"❌ Error processing {game_key}: {e}")
        import traceback
        traceback.print_exc()


def run_loop():
    print("🏀 Live parser started - monitoring game_schedule")
    while True:
        games = fetch_active_games()
        if not games:
            print("⚠️ No active games in schedule")
        else:
            print(f"📡 Polling {len(games)} games...")
        
        for g in games:
            process_game(g)
        
        time.sleep(POLL_INTERVAL)


if __name__ == "__main__":
    run_loop()
