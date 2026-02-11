"""
Team Offense Distribution Calculator

This module calculates how a team's offense is distributed across its players.
It answers the question: "What percentage of the team's total possessions used
does each player account for?"

IMPORTANT DISTINCTION:
- This is "Team Offense Distribution" (team-level metric)
- It is NOT the same as classic per-player Usage% (USG%)
- Classic USG% measures what % of team plays a player uses WHILE ON THE COURT
- This metric measures what % of team offense each player is responsible for
  across ALL games in a season/league

Formula:
  player_possessions_used = SUM(FGA + 0.44 * FTA + Turnovers) across all games
  team_possessions_used = SUM(player_possessions_used) for all players on team
  offensive_share = player_possessions_used / team_possessions_used
  offensive_share_pct = 100 * offensive_share

The sum of offensive_share_pct for all players on a team should equal ~100%.

Use case: Supports teamwork/offensive balance analysis. Shows if a team relies
heavily on one player or spreads the ball around.
"""

from app.utils.json_parser import supabase


def get_team_offense_distribution(
    league_id: str,
    team_id: str | None = None,
) -> list[dict]:
    """
    Calculate offensive distribution for teams in a league.
    
    Args:
        league_id: The league to analyze
        team_id: Optional - filter to a single team. If None, returns all teams.
    
    Returns:
        List of dicts with:
        - team_id
        - team_name
        - player_id
        - player_name
        - possessions_used (player's total possessions used)
        - team_possessions_used (team's total possessions used)
        - offensive_share (decimal 0-1)
        - offensive_share_pct (0-100%)
        
        Results are ordered by team_id, then offensive_share DESC.
    """
    
    query = supabase.table("player_stats").select(
        "team_id, team_name, player_id, full_name, "
        "sfieldgoalsattempted, sfreethrowsattempted, sturnovers"
    ).eq("league_id", league_id)
    
    if team_id:
        query = query.eq("team_id", team_id)
    
    result = query.execute()
    
    if not result.data:
        return []
    
    player_totals = {}
    team_totals = {}
    
    for row in result.data:
        tid = row["team_id"]
        pid = row["player_id"]
        
        fga = row.get("sfieldgoalsattempted") or 0
        fta = row.get("sfreethrowsattempted") or 0
        tov = row.get("sturnovers") or 0
        
        possessions_used = fga + (0.44 * fta) + tov
        
        key = (tid, pid)
        if key not in player_totals:
            player_totals[key] = {
                "team_id": tid,
                "team_name": row.get("team_name"),
                "player_id": pid,
                "player_name": row.get("full_name"),
                "possessions_used": 0,
            }
        player_totals[key]["possessions_used"] += possessions_used
        
        if tid not in team_totals:
            team_totals[tid] = 0
        team_totals[tid] += possessions_used
    
    results = []
    for key, player in player_totals.items():
        tid = player["team_id"]
        team_poss = team_totals.get(tid, 0)
        
        if team_poss > 0:
            offensive_share = player["possessions_used"] / team_poss
            offensive_share_pct = 100 * offensive_share
        else:
            offensive_share = 0
            offensive_share_pct = 0
        
        results.append({
            "team_id": tid,
            "team_name": player["team_name"],
            "player_id": player["player_id"],
            "player_name": player["player_name"],
            "possessions_used": round(player["possessions_used"], 2),
            "team_possessions_used": round(team_poss, 2),
            "offensive_share": round(offensive_share, 4),
            "offensive_share_pct": round(offensive_share_pct, 2),
        })
    
    results.sort(key=lambda x: (x["team_id"], -x["offensive_share"]))
    
    return results


def test_team_offense_distribution():
    """
    Debug/test function to verify the calculation works correctly.
    Prints offensive distribution for one league and verifies totals sum to ~100%.
    """
    print("=" * 70)
    print("TEAM OFFENSE DISTRIBUTION TEST")
    print("=" * 70)
    
    leagues_result = supabase.table("leagues").select("league_id, name").limit(1).execute()
    if not leagues_result.data:
        print("No leagues found!")
        return
    
    league = leagues_result.data[0]
    league_id = league["league_id"]
    league_name = league["name"]
    
    print(f"\nLeague: {league_name}")
    print(f"League ID: {league_id}")
    
    results = get_team_offense_distribution(league_id)
    
    if not results:
        print("No player stats found for this league!")
        return
    
    current_team = None
    team_sum = 0
    
    for row in results:
        if row["team_id"] != current_team:
            if current_team is not None:
                print(f"  {'=' * 40}")
                print(f"  TEAM TOTAL: {team_sum:.2f}% (should be ~100%)")
                print()
            
            current_team = row["team_id"]
            team_sum = 0
            print(f"\n📊 {row['team_name']}")
            print(f"   Team Possessions Used: {row['team_possessions_used']}")
            print("-" * 50)
        
        team_sum += row["offensive_share_pct"]
        print(f"  {row['player_name']:30} | {row['offensive_share_pct']:6.2f}% | {row['possessions_used']:.1f} poss")
    
    if current_team is not None:
        print(f"  {'=' * 40}")
        print(f"  TEAM TOTAL: {team_sum:.2f}% (should be ~100%)")
    
    print("\n" + "=" * 70)
    print("TEST COMPLETE")
    print("=" * 70)


if __name__ == "__main__":
    test_team_offense_distribution()
