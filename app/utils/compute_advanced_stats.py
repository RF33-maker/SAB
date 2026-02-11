from app.utils.supabase_queries import supabase
from app.utils.advanced_team_stats import (
    fetch_team_stats_for_league,
    compute_team_advanced
)
from app.utils.advanced_player_stats import (
    fetch_player_stats_for_league,
    compute_player_advanced
)


def build_team_context(team_rows):
    """
    Create a team_map structure for player calculations
    
    For each team, finds its opponent in the same game and creates a context object
    with team stats, opponent stats, and possession data.
    
    Validates that:
    - Each game has exactly 2 teams
    - Both teams have possessions calculated
    
    Args:
        team_rows: List of team_stats rows from Supabase (with advanced stats computed)
    
    Returns:
        team_map: Dict mapping game_key -> {team_id -> team_stats_row}
                  This allows players to find their team and opponent stats by game_key
    """
    team_map = {}
    skipped_games = 0
    
    # First pass: organize by game_key
    game_dict = {}
    for team_row in team_rows:
        game_key = team_row.get("game_key")
        team_id = team_row.get("team_id")
        
        if not game_key or not team_id:
            continue
        
        if game_key not in game_dict:
            game_dict[game_key] = {}
        
        game_dict[game_key][team_id] = team_row
    
    # Second pass: validate and build team_map structure
    for game_key, teams in game_dict.items():
        # Validate each game has exactly 2 teams
        if len(teams) != 2:
            print(f"   ⚠️  Skipping game '{game_key}': found {len(teams)} teams (expected 2)")
            skipped_games += 1
            continue
        
        # Validate both teams have possessions calculated
        teams_with_possessions = 0
        for team_id, team_row in teams.items():
            poss = team_row.get("possessions")
            if poss is not None and poss > 0:
                teams_with_possessions += 1
        
        if teams_with_possessions != 2:
            print(f"   ⚠️  Skipping game '{game_key}': only {teams_with_possessions}/2 teams have possessions")
            skipped_games += 1
            continue
        
        # Game is valid, add to team_map
        team_map[game_key] = teams
    
    if skipped_games > 0:
        print(f"   ⚠️  Skipped {skipped_games} games due to validation failures")
    
    return team_map


def compute_advanced_stats(league_id):
    """
    Main coordinator function to compute both team and player advanced stats
    
    Flow:
    1. Fetch team stats
    2. Compute team advanced stats (possessions, ratings, etc.)
    3. Re-fetch updated team stats (now includes possessions)
    4. Build team_map for player context
    5. Fetch player stats
    6. Compute player advanced stats using team_map
    
    Args:
        league_id: The league ID to process
    
    Returns:
        Dict with status, counts, and processing details
    """
    print(f"\n🔧 Computing advanced stats for league: {league_id}")
    
    try:
        # Step 1: Fetch all team rows
        print("   📊 Step 1: Fetching team stats...")
        team_rows = fetch_team_stats_for_league(league_id)
        
        if not team_rows:
            print("   ⚠️  No team stats found for this league")
            return {"status": "no_data", "teams_processed": 0, "players_processed": 0}
        
        print(f"   Found {len(team_rows)} team stat records")
        
        # Step 2: Compute TEAM advanced stats
        print("   📊 Step 2: Computing team advanced stats...")
        teams_processed = compute_team_advanced(team_rows)
        print(f"   Team stats processed: {teams_processed}")
        
        if teams_processed == 0:
            print("   ❌ No team stats were successfully processed")
            return {
                "status": "team_computation_failed",
                "teams_processed": 0,
                "players_processed": 0,
                "error": "Team advanced stats computation produced no results"
            }
        
        # Step 3: Re-fetch UPDATED team stats (to include calculated fields like possessions)
        print("   📊 Step 3: Re-fetching updated team stats...")
        updated_team_rows = fetch_team_stats_for_league(league_id)
        
        if not updated_team_rows:
            print("   ❌ Failed to re-fetch team stats")
            return {
                "status": "refetch_failed",
                "teams_processed": teams_processed,
                "players_processed": 0,
                "error": "Could not re-fetch updated team stats"
            }
        
        # Validate that updated rows have possessions
        rows_with_possessions = sum(1 for row in updated_team_rows if row.get("possessions"))
        if rows_with_possessions == 0:
            print("   ❌ Re-fetched team stats do not contain possessions data")
            return {
                "status": "missing_possessions",
                "teams_processed": teams_processed,
                "players_processed": 0,
                "error": "Updated team stats missing possessions - team computation may have failed"
            }
        
        print(f"   Found {rows_with_possessions}/{len(updated_team_rows)} teams with possessions")
        
        # Step 4: Build team_map
        print("   📊 Step 4: Building team context map...")
        team_map = build_team_context(updated_team_rows)
        print(f"   Built team_map with {len(team_map)} valid games")
        
        if not team_map:
            print("   ⚠️  No valid games in team_map - skipping player stats")
            return {
                "status": "no_valid_games",
                "teams_processed": teams_processed,
                "players_processed": 0,
                "error": "No games passed validation for player context"
            }
        
        # Step 5: Fetch all PLAYER rows
        print("   📊 Step 5: Fetching player stats...")
        player_rows = fetch_player_stats_for_league(league_id)
        
        if not player_rows:
            print("   ⚠️  No player stats found for this league")
            return {
                "status": "success",
                "teams_processed": teams_processed,
                "players_processed": 0,
                "message": "Team stats computed, no players found"
            }
        
        print(f"   Found {len(player_rows)} player stat records")
        
        # Step 6: Compute PLAYER advanced stats (using team_map)
        print("   📊 Step 6: Computing player advanced stats...")
        players_processed = compute_player_advanced(player_rows, team_map)
        print(f"   Player stats processed: {players_processed}")
        
        # Step 7: Return summary
        print(f"   ✅ Advanced stats computation complete!")
        return {
            "status": "success",
            "teams_processed": teams_processed,
            "players_processed": players_processed,
            "total_teams": len(team_rows),
            "total_players": len(player_rows),
            "valid_games": len(team_map)
        }
        
    except Exception as e:
        print(f"   ❌ Error in compute_advanced_stats: {e}")
        import traceback
        traceback.print_exc()
        return {
            "status": "error",
            "teams_processed": 0,
            "players_processed": 0,
            "error": str(e)
        }
