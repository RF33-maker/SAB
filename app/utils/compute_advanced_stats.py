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
    
    Args:
        team_rows: List of team_stats rows from Supabase (with advanced stats computed)
    
    Returns:
        team_map: Dict mapping game_key -> {team_id -> team_stats_row}
                  This allows players to find their team and opponent stats by game_key
    """
    team_map = {}
    
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
    
    # Second pass: build team_map structure
    # team_map[game_key][team_id] = team_stats_row
    team_map = game_dict
    
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
        Dict with status and counts
    """
    print(f"\n🔧 Computing advanced stats for league: {league_id}")
    
    # Step 1: Fetch all team rows
    print("   📊 Step 1: Fetching team stats...")
    team_rows = fetch_team_stats_for_league(league_id)
    
    if not team_rows:
        print("   ⚠️  No team stats found for this league")
        return {"status": "no_data", "teams": 0, "players": 0}
    
    print(f"   Found {len(team_rows)} team stat records")
    
    # Step 2: Compute TEAM advanced stats
    print("   📊 Step 2: Computing team advanced stats...")
    compute_team_advanced(team_rows)
    
    # Step 3: Re-fetch UPDATED team stats (to include calculated fields like possessions)
    print("   📊 Step 3: Re-fetching updated team stats...")
    updated_team_rows = fetch_team_stats_for_league(league_id)
    
    # Step 4: Build team_map
    print("   📊 Step 4: Building team context map...")
    team_map = build_team_context(updated_team_rows)
    print(f"   Built team_map with {len(team_map)} games")
    
    # Step 5: Fetch all PLAYER rows
    print("   📊 Step 5: Fetching player stats...")
    player_rows = fetch_player_stats_for_league(league_id)
    
    if not player_rows:
        print("   ⚠️  No player stats found for this league")
        return {"status": "success", "teams": len(team_rows), "players": 0}
    
    print(f"   Found {len(player_rows)} player stat records")
    
    # Step 6: Compute PLAYER advanced stats (using team_map)
    print("   📊 Step 6: Computing player advanced stats...")
    compute_player_advanced(player_rows, team_map)
    
    # Step 7: Return summary
    print(f"   ✅ Advanced stats computation complete!")
    return {
        "status": "success",
        "teams": len(team_rows),
        "players": len(player_rows)
    }
