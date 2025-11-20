import math
from app.utils.supabase_queries import supabase

def safe_div(n, d):
    return n / d if d not in (0, None) else 0


def convert_minutes_to_decimal(minutes_str):
    """
    Convert minutes from "MM:SS" format to decimal
    Example: "32:15" -> 32.25
    """
    if not minutes_str or minutes_str == "0":
        return 0.0
    
    try:
        if isinstance(minutes_str, (int, float)):
            return float(minutes_str)
        
        if ":" in str(minutes_str):
            parts = str(minutes_str).split(":")
            try:
                mins = int(parts[0])
                secs = int(parts[1]) if len(parts) > 1 else 0
                return mins + (secs / 60.0)
            except ValueError as e:
                print(f"   ⚠️  Invalid minutes format '{minutes_str}': {e}")
                return 0.0
        else:
            return float(minutes_str)
    except Exception as e:
        print(f"   ⚠️  Error converting minutes '{minutes_str}': {e}")
        return 0.0


def calc_player_efg(player):
    """
    Calculate effective field goal percentage
    eFG% = (FGM + 0.5 * 3PM) / FGA
    """
    fgm = player.get("sfieldgoalsmade", 0) or 0
    tpm = player.get("sthreepointersmade", 0) or 0
    fga = player.get("sfieldgoalsattempted", 0) or 0
    
    return safe_div(fgm + 0.5 * tpm, fga) * 100


def calc_player_ts(player):
    """
    Calculate true shooting percentage
    TS% = PTS / (2 * (FGA + 0.44 * FTA))
    """
    pts = player.get("spoints", 0) or 0
    fga = player.get("sfieldgoalsattempted", 0) or 0
    fta = player.get("sfreethrowsattempted", 0) or 0
    
    return safe_div(pts, 2 * (fga + 0.44 * fta)) * 100


def calc_player_three_point_rate(player):
    """
    Calculate three-point attempt rate
    3PA Rate = 3PA / FGA
    """
    tpa = player.get("sthreepointersattempted", 0) or 0
    fga = player.get("sfieldgoalsattempted", 0) or 0
    
    return safe_div(tpa, fga) * 100


def calc_player_usage(player, team_poss, team_totals):
    """
    Calculate usage percentage
    USG% = (PlayerFormula * TeamMinutes) / (PlayerMinutes * team_poss)
    Where PlayerFormula = FGA + 0.44 * FTA + TOV
    
    Team minutes is calculated from actual game duration (default 40 min per quarter * number of players)
    """
    fga = player.get("sfieldgoalsattempted", 0) or 0
    fta = player.get("sfreethrowsattempted", 0) or 0
    tov = player.get("sturnovers", 0) or 0
    
    player_formula = fga + 0.44 * fta + tov
    player_minutes = convert_minutes_to_decimal(player.get("sminutes", 0))
    
    # Calculate team minutes: 5 players * game duration (40 min default)
    # If team has actual minutes data, use 5x that; otherwise default to 200
    team_minutes = 200.0  # Default: 5 players * 40 min
    
    if player_minutes == 0 or team_poss == 0:
        return 0.0
    
    usg = (player_formula * team_minutes) / (player_minutes * team_poss)
    return usg * 100  # Return as percentage


def calc_player_possessions(player):
    """
    Estimate player possessions
    player_possessions = FGA + 0.44 * FTA + TOV
    """
    fga = player.get("sfieldgoalsattempted", 0) or 0
    fta = player.get("sfreethrowsattempted", 0) or 0
    tov = player.get("sturnovers", 0) or 0
    
    return fga + 0.44 * fta + tov


def calc_player_ast_percent(player, team_totals):
    """
    Calculate assist percentage
    AST% = AST / (team_FGM - player_FGM)
    """
    player_ast = player.get("sassists", 0) or 0
    player_fgm = player.get("sfieldgoalsmade", 0) or 0
    team_fgm = team_totals.get("tot_sfieldgoalsmade", 0) or 0
    
    return safe_div(player_ast, team_fgm - player_fgm) * 100


def calc_player_rebound_percentages(player, team_totals, opp_totals):
    """
    Calculate rebounding percentages
    OREB% = player_ORB / (player_ORB + opp_DRB)
    DREB% = player_DRB / (player_DRB + opp_ORB)
    REB%  = player_REB / (player_REB + opp_REB)
    """
    player_orb = player.get("sreboundsoffensive", 0) or 0
    player_reb = player.get("sreboundstotal", 0) or 0
    player_drb = player_reb - player_orb
    
    team_orb = team_totals.get("tot_sreboundsoffensive", 0) or 0
    team_reb = team_totals.get("tot_sreboundstotal", 0) or 0
    team_drb = team_reb - team_orb
    
    opp_orb = opp_totals.get("tot_sreboundsoffensive", 0) or 0
    opp_reb = opp_totals.get("tot_sreboundstotal", 0) or 0
    opp_drb = opp_reb - opp_orb
    
    oreb_pct = safe_div(player_orb, player_orb + opp_drb) * 100
    dreb_pct = safe_div(player_drb, player_drb + opp_orb) * 100
    reb_pct = safe_div(player_reb, player_reb + opp_reb) * 100
    
    return {
        "oreb_percent": oreb_pct,
        "dreb_percent": dreb_pct,
        "reb_percent": reb_pct
    }


def calc_player_tov_percent(player):
    """
    Calculate turnover percentage
    TOV% = TOV / (FGA + 0.44 * FTA + AST + TOV)
    """
    tov = player.get("sturnovers", 0) or 0
    fga = player.get("sfieldgoalsattempted", 0) or 0
    fta = player.get("sfreethrowsattempted", 0) or 0
    ast = player.get("sassists", 0) or 0
    
    return safe_div(tov, fga + 0.44 * fta + ast + tov) * 100


def calc_player_pie(player, team_totals, opp_totals):
    """
    Calculate Player Impact Estimate
    PIE = (Player positive - Player negative) / (Team positive + Team negative + Opp positive + Opp negative)
    
    Positive: PTS + FGM + FTM + OREB + AST + STL + BLK
    Negative: FGA - FGM + FTA - FTM + TOV + PF
    """
    # Player positive actions
    player_pts = player.get("spoints", 0) or 0
    player_fgm = player.get("sfieldgoalsmade", 0) or 0
    player_ftm = player.get("sfreethrowsmade", 0) or 0
    player_orb = player.get("sreboundsoffensive", 0) or 0
    player_ast = player.get("sassists", 0) or 0
    player_stl = player.get("ssteals", 0) or 0
    player_blk = player.get("sblockshots", 0) or 0
    
    player_positive = player_pts + player_fgm + player_ftm + player_orb + player_ast + player_stl + player_blk
    
    # Player negative actions
    player_fga = player.get("sfieldgoalsattempted", 0) or 0
    player_fta = player.get("sfreethrowsattempted", 0) or 0
    player_tov = player.get("sturnovers", 0) or 0
    player_pf = player.get("spersonalfouls", 0) or 0
    
    player_negative = (player_fga - player_fgm) + (player_fta - player_ftm) + player_tov + player_pf
    
    # Team positive actions
    team_pts = team_totals.get("tot_spoints", 0) or 0
    team_fgm = team_totals.get("tot_sfieldgoalsmade", 0) or 0
    team_ftm = team_totals.get("tot_sfreethrowsmade", 0) or 0
    team_orb = team_totals.get("tot_sreboundsoffensive", 0) or 0
    team_ast = team_totals.get("tot_sassists", 0) or 0
    team_stl = team_totals.get("tot_ssteals", 0) or 0
    team_blk = team_totals.get("tot_sblockshots", 0) or 0
    
    team_positive = team_pts + team_fgm + team_ftm + team_orb + team_ast + team_stl + team_blk
    
    # Team negative actions
    team_fga = team_totals.get("tot_sfieldgoalsattempted", 0) or 0
    team_fta = team_totals.get("tot_sfreethrowsattempted", 0) or 0
    team_tov = team_totals.get("tot_sturnovers", 0) or 0
    team_pf = team_totals.get("tot_spersonalfouls", 0) or 0
    
    team_negative = (team_fga - team_fgm) + (team_fta - team_ftm) + team_tov + team_pf
    
    # Opponent positive actions
    opp_pts = opp_totals.get("tot_spoints", 0) or 0
    opp_fgm = opp_totals.get("tot_sfieldgoalsmade", 0) or 0
    opp_ftm = opp_totals.get("tot_sfreethrowsmade", 0) or 0
    opp_orb = opp_totals.get("tot_sreboundsoffensive", 0) or 0
    opp_ast = opp_totals.get("tot_sassists", 0) or 0
    opp_stl = opp_totals.get("tot_ssteals", 0) or 0
    opp_blk = opp_totals.get("tot_sblockshots", 0) or 0
    
    opp_positive = opp_pts + opp_fgm + opp_ftm + opp_orb + opp_ast + opp_stl + opp_blk
    
    # Opponent negative actions
    opp_fga = opp_totals.get("tot_sfieldgoalsattempted", 0) or 0
    opp_fta = opp_totals.get("tot_sfreethrowsattempted", 0) or 0
    opp_tov = opp_totals.get("tot_sturnovers", 0) or 0
    opp_pf = opp_totals.get("tot_spersonalfouls", 0) or 0
    
    opp_negative = (opp_fga - opp_fgm) + (opp_fta - opp_ftm) + opp_tov + opp_pf
    
    return safe_div(player_positive - player_negative, team_positive + team_negative + opp_positive + opp_negative) * 100


def calc_player_ratings_estimated(player, team_stats, opp_stats):
    """
    Estimate offensive and defensive ratings
    
    OffRtg = (PTS / player_possessions) * 100
    DefRtg = (opp_points / team_possessions) * 100
    NetRtg = OffRtg - DefRtg
    """
    player_pts = player.get("spoints", 0) or 0
    player_poss = calc_player_possessions(player)
    
    opp_pts = opp_stats.get("tot_spoints", 0) or 0
    team_poss = team_stats.get("possessions", 0) or 0
    
    off_rtg = safe_div(player_pts, player_poss) * 100 if player_poss > 0 else 0
    def_rtg = safe_div(opp_pts, team_poss) * 100 if team_poss > 0 else 0
    net_rtg = off_rtg - def_rtg
    
    return {
        "off_rating": off_rtg,
        "def_rating": def_rtg,
        "net_rating": net_rtg
    }


def calc_player_scoring_distribution(player):
    """
    Calculate scoring distribution percentages
    Returns breakdown of points from different sources
    """
    pts_2pt = (player.get("stwopointersmade", 0) or 0) * 2
    pts_3pt = (player.get("sthreepointersmade", 0) or 0) * 3
    pts_ft = player.get("sfreethrowsmade", 0) or 0
    total_pts = player.get("spoints", 0) or 0
    
    pts_paint = player.get("spointsinthepaint", 0) or 0
    pts_fast = player.get("spointsfastbreak", 0) or 0
    pts_2nd = player.get("spointssecondchance", 0) or 0
    pts_off_to = player.get("spointsfromturnovers", 0) or 0
    
    # Midrange = 2PT points - Paint points (clamped)
    pts_midrange = max(0, pts_2pt - pts_paint)
    
    return {
        "pts_percent_2pt": safe_div(pts_2pt, total_pts) * 100,
        "pts_percent_3pt": safe_div(pts_3pt, total_pts) * 100,
        "pts_percent_ft": safe_div(pts_ft, total_pts) * 100,
        "pts_percent_midrange": safe_div(pts_midrange, total_pts) * 100,
        "pts_percent_paint": safe_div(pts_paint, total_pts) * 100,
        "pts_percent_fastbreak": safe_div(pts_fast, total_pts) * 100,
        "pts_percent_second_chance": safe_div(pts_2nd, total_pts) * 100,
        "pts_percent_off_turnovers": safe_div(pts_off_to, total_pts) * 100
    }


def fetch_player_stats_for_league(league_id):
    """
    Fetch all player_stats rows for a given league
    """
    try:
        result = supabase.table("player_stats").select("*").eq("league_id", league_id).execute()
        return result.data if result.data else []
    except Exception as e:
        print(f"Error fetching player stats for league {league_id}: {e}")
        return []


def write_player_advanced_to_supabase(player_id, data):
    """
    Write advanced stats to player_stats table in Supabase
    Returns True on success, False on failure
    """
    try:
        result = supabase.table("player_stats").update(data).eq("id", player_id).execute()
        if result and result.data:
            return True
        else:
            print(f"   ⚠️  No data returned when writing stats for player_id {player_id}")
            return False
    except Exception as e:
        print(f"   ❌ Error writing advanced stats for player_id {player_id}: {e}")
        import traceback
        traceback.print_exc()
        return False


def compute_player_advanced(player_rows, team_map):
    """
    Main function to compute all advanced player metrics
    
    For each player:
    1. Find their team totals via team_map[player["team_id"]]
    2. Find opponent totals via same game_key
    3. Calculate all advanced metrics
    4. Write back to Supabase
    
    Args:
        player_rows: List of player_stats rows from Supabase
        team_map: Dict mapping game_key -> {team_id -> team_stats_row}
    """
    processed = 0
    skipped = 0
    write_failures = 0
    
    for player in player_rows:
        game_key = player.get("game_key")
        team_id = player.get("team_id")
        player_id = player.get("id")
        
        if not game_key or not team_id or not player_id:
            skipped += 1
            continue
        
        # Get team and opponent data from team_map
        if game_key not in team_map:
            skipped += 1
            print(f"   ⚠️  Skipping player {player.get('name', 'unknown')}: game_key '{game_key}' not in team_map")
            continue
        
        game_teams = team_map[game_key]
        
        if team_id not in game_teams:
            skipped += 1
            print(f"   ⚠️  Skipping player {player.get('name', 'unknown')}: team_id '{team_id}' not found in game")
            continue
        
        team_stats = game_teams[team_id]
        
        # Find opponent (the other team in the game)
        opp_stats = None
        for tid, tstats in game_teams.items():
            if tid != team_id:
                opp_stats = tstats
                break
        
        if not opp_stats:
            skipped += 1
            print(f"   ⚠️  Skipping player {player.get('name', 'unknown')}: no opponent found")
            continue
        
        # Calculate all advanced metrics
        team_poss = team_stats.get("possessions", 0) or 0
        
        efg = calc_player_efg(player)
        ts = calc_player_ts(player)
        three_pt_rate = calc_player_three_point_rate(player)
        usage = calc_player_usage(player, team_poss, team_stats)
        player_poss = calc_player_possessions(player)
        ast_pct = calc_player_ast_percent(player, team_stats)
        reb_pcts = calc_player_rebound_percentages(player, team_stats, opp_stats)
        tov_pct = calc_player_tov_percent(player)
        pie = calc_player_pie(player, team_stats, opp_stats)
        ratings = calc_player_ratings_estimated(player, team_stats, opp_stats)
        scoring = calc_player_scoring_distribution(player)
        
        # Combine all fields
        updated_fields = {
            "player_possessions": player_poss,
            "efg_percent": efg,
            "ts_percent": ts,
            "three_point_rate": three_pt_rate,
            "usage_percent": usage,
            "ast_percent": ast_pct,
            "oreb_percent": reb_pcts["oreb_percent"],
            "dreb_percent": reb_pcts["dreb_percent"],
            "reb_percent": reb_pcts["reb_percent"],
            "tov_percent": tov_pct,
            "pie": pie,
            "off_rating": ratings["off_rating"],
            "def_rating": ratings["def_rating"],
            "net_rating": ratings["net_rating"],
            "pts_percent_2pt": scoring["pts_percent_2pt"],
            "pts_percent_3pt": scoring["pts_percent_3pt"],
            "pts_percent_ft": scoring["pts_percent_ft"],
            "pts_percent_midrange": scoring["pts_percent_midrange"],
            "pts_percent_pitp": scoring["pts_percent_paint"],
            "pts_percent_fastbreak": scoring["pts_percent_fastbreak"],
            "pts_percent_second_chance": scoring["pts_percent_second_chance"],
            "pts_percent_off_turnovers": scoring["pts_percent_off_turnovers"]
        }
        
        # Write to Supabase
        success = write_player_advanced_to_supabase(player_id, updated_fields)
        if success:
            processed += 1
        else:
            write_failures += 1
    
    print(f"   ✅ Processed {processed} players, skipped {skipped}, write failures {write_failures}")
    return processed
