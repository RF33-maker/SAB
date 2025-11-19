from app.utils.supabase_queries import supabase


def safe_div(n, d):
    """Safely divide two numbers, returning 0 if denominator is 0 or None"""
    return n / d if d not in (0, None) else 0


def calculate_possessions(team):
    """
    Calculate team possessions using the standard formula:
    Possessions = FGA + 0.4 * FTA - ORB + TOV
    """
    fga = team.get("tot_sfieldgoalsattempted", 0) or 0
    fta = team.get("tot_sfreethrowsattempted", 0) or 0
    orb = team.get("tot_sreboundsoffensive", 0) or 0
    tov = team.get("tot_sturnovers", 0) or 0
    
    return fga + 0.4 * fta - orb + tov


def calculate_opp_possessions(opp):
    """Calculate opponent possessions using same formula"""
    return calculate_possessions(opp)


def calculate_off_rating(points, poss):
    """
    Calculate offensive rating: points per 100 possessions
    OffRating = (Points / Possessions) * 100
    """
    return safe_div(points, poss) * 100


def calculate_def_rating(opp_points, opp_poss):
    """
    Calculate defensive rating: opponent points per 100 possessions
    DefRating = (OppPoints / OppPossessions) * 100
    """
    return safe_div(opp_points, opp_poss) * 100


def calculate_net_rating(off, defn):
    """Calculate net rating: offensive rating - defensive rating"""
    return off - defn


def calculate_pace(team_poss, opp_poss, minutes=200):
    """
    Calculate pace: estimated possessions per 40 minutes
    Pace = 40 * ((TeamPoss + OppPoss) / (2 * Minutes))
    Default minutes = 200 (5 players * 40 minutes)
    """
    return 40 * safe_div(team_poss + opp_poss, 2 * minutes)


def calculate_efg(team):
    """
    Calculate effective field goal percentage
    eFG% = (FGM + 0.5 * 3PM) / FGA
    """
    fgm = team.get("tot_sfieldgoalsmade", 0) or 0
    tpm = team.get("tot_sthreepointersmade", 0) or 0
    fga = team.get("tot_sfieldgoalsattempted", 0) or 0
    
    return safe_div(fgm + 0.5 * tpm, fga) * 100


def calculate_ts(team):
    """
    Calculate true shooting percentage
    TS% = Points / (2 * (FGA + 0.44 * FTA))
    """
    points = team.get("tot_spoints", 0) or 0
    fga = team.get("tot_sfieldgoalsattempted", 0) or 0
    fta = team.get("tot_sfreethrowsattempted", 0) or 0
    
    return safe_div(points, 2 * (fga + 0.44 * fta)) * 100


def calculate_rebound_percentages(team, opp):
    """
    Calculate rebounding percentages
    OREB% = TeamORB / (TeamORB + OppDRB)
    DREB% = TeamDRB / (TeamDRB + OppORB)
    REB% = TeamREB / (TeamREB + OppREB)
    """
    team_orb = team.get("tot_sreboundsoffensive", 0) or 0
    team_drb = (team.get("tot_sreboundstotal", 0) or 0) - team_orb
    team_reb = team.get("tot_sreboundstotal", 0) or 0
    
    opp_orb = opp.get("tot_sreboundsoffensive", 0) or 0
    opp_drb = (opp.get("tot_sreboundstotal", 0) or 0) - opp_orb
    opp_reb = opp.get("tot_sreboundstotal", 0) or 0
    
    oreb_pct = safe_div(team_orb, team_orb + opp_drb) * 100
    dreb_pct = safe_div(team_drb, team_drb + opp_orb) * 100
    reb_pct = safe_div(team_reb, team_reb + opp_reb) * 100
    
    return {
        "oreb_percent": oreb_pct,
        "dreb_percent": dreb_pct,
        "reb_percent": reb_pct,
        "opp_oreb_percent": safe_div(opp_orb, opp_orb + team_drb) * 100
    }


def calculate_ast_percent(team):
    """
    Calculate assist percentage
    AST% = Assists / Field Goals Made
    """
    team_ast = team.get("tot_sassists", 0) or 0
    team_fgm = team.get("tot_sfieldgoalsmade", 0) or 0
    
    return safe_div(team_ast, team_fgm) * 100


def calculate_turnover_percentages(team, opp, team_poss, opp_poss):
    """
    Calculate turnover percentages
    TOV% = Turnovers / Possessions
    """
    team_tov = team.get("tot_sturnovers", 0) or 0
    opp_tov = opp.get("tot_sturnovers", 0) or 0
    
    return {
        "tov_percent": safe_div(team_tov, team_poss) * 100,
        "opp_tov_percent": safe_div(opp_tov, opp_poss) * 100
    }


def calculate_scoring_distribution(team):
    """
    Calculate scoring distribution percentages
    Returns breakdown of points from different sources
    """
    points_2pt = (team.get("tot_stwopointersmade", 0) or 0) * 2
    points_3pt = (team.get("tot_sthreepointersmade", 0) or 0) * 3
    points_ft = team.get("tot_sfreethrowsmade", 0) or 0
    total_points = team.get("tot_spoints", 0) or 0
    
    # Paint, fastbreak, second chance, and off turnovers if available
    points_pitp = team.get("tot_spointspaint", 0) or 0
    points_fb = team.get("tot_spointsfastbreak", 0) or 0
    points_2nd = team.get("tot_spointssecondchance", 0) or 0
    points_off_to = team.get("tot_spointsoffturnover", 0) or 0
    
    return {
        "pts_percent_2pt": safe_div(points_2pt, total_points) * 100,
        "pts_percent_3pt": safe_div(points_3pt, total_points) * 100,
        "pts_percent_ft": safe_div(points_ft, total_points) * 100,
        "pts_percent_pitp": safe_div(points_pitp, total_points) * 100,
        "pts_percent_fastbreak": safe_div(points_fb, total_points) * 100,
        "pts_percent_second_chance": safe_div(points_2nd, total_points) * 100,
        "pts_percent_off_turnovers": safe_div(points_off_to, total_points) * 100
    }


def calculate_four_factors(team, opp, team_poss, opp_poss):
    """
    Calculate the Four Factors of basketball success
    1. Shooting (eFG%)
    2. Turnovers (TOV%)
    3. Rebounding (OREB%)
    4. Free Throws (FT Rate)
    """
    # Team factors
    efg = calculate_efg(team)
    reb_pcts = calculate_rebound_percentages(team, opp)
    tov_pcts = calculate_turnover_percentages(team, opp, team_poss, opp_poss)
    
    team_fta = team.get("tot_sfreethrowsattempted", 0) or 0
    team_fga = team.get("tot_sfieldgoalsattempted", 0) or 0
    ft_rate = safe_div(team_fta, team_fga) * 100
    
    # Opponent factors
    opp_efg = calculate_efg(opp)
    opp_fta = opp.get("tot_sfreethrowsattempted", 0) or 0
    opp_fga = opp.get("tot_sfieldgoalsattempted", 0) or 0
    opp_ft_rate = safe_div(opp_fta, opp_fga) * 100
    
    return {
        "efg_percent": efg,
        "ft_rate": ft_rate,
        "tov_percent": tov_pcts["tov_percent"],
        "oreb_percent": reb_pcts["oreb_percent"],
        "opp_efg_percent": opp_efg,
        "opp_ft_rate": opp_ft_rate,
        "opp_tov_percent": tov_pcts["opp_tov_percent"],
        "opp_oreb_percent": reb_pcts["opp_oreb_percent"]
    }


def calculate_three_point_rate(team):
    """
    Calculate three point attempt rate
    3PAr = 3PA / FGA
    """
    tpa = team.get("tot_sthreepointersattempted", 0) or 0
    fga = team.get("tot_sfieldgoalsattempted", 0) or 0
    return safe_div(tpa, fga) * 100


def calculate_ast_to_ratio(team):
    """
    Calculate assist to turnover ratio
    AST/TO = Assists / Turnovers
    """
    assists = team.get("tot_sassists", 0) or 0
    turnovers = team.get("tot_sturnovers", 0) or 0
    return safe_div(assists, turnovers)


def calculate_pie(team, opp):
    """
    Calculate Player/Team Impact Estimate (PIE)
    Simplified team version
    """
    team_pts = team.get("tot_spoints", 0) or 0
    team_fgm = team.get("tot_sfieldgoalsmade", 0) or 0
    team_ftm = team.get("tot_sfreethrowsmade", 0) or 0
    team_fga = team.get("tot_sfieldgoalsattempted", 0) or 0
    team_fta = team.get("tot_sfreethrowsattempted", 0) or 0
    team_reb = team.get("tot_sreboundstotal", 0) or 0
    team_ast = team.get("tot_sassists", 0) or 0
    team_stl = team.get("tot_ssteals", 0) or 0
    team_blk = team.get("tot_sblocks", 0) or 0
    team_tov = team.get("tot_sturnovers", 0) or 0
    team_pf = team.get("tot_sfoulspersonal", 0) or 0
    
    opp_pts = opp.get("tot_spoints", 0) or 0
    opp_fgm = opp.get("tot_sfieldgoalsmade", 0) or 0
    opp_ftm = opp.get("tot_sfreethrowsmade", 0) or 0
    opp_fga = opp.get("tot_sfieldgoalsattempted", 0) or 0
    opp_fta = opp.get("tot_sfreethrowsattempted", 0) or 0
    opp_reb = opp.get("tot_sreboundstotal", 0) or 0
    opp_ast = opp.get("tot_sassists", 0) or 0
    opp_stl = opp.get("tot_ssteals", 0) or 0
    opp_blk = opp.get("tot_sblocks", 0) or 0
    opp_tov = opp.get("tot_sturnovers", 0) or 0
    opp_pf = opp.get("tot_sfoulspersonal", 0) or 0
    
    team_score = (team_pts + team_fgm + team_ftm - team_fga - team_fta + 
                  team_reb + team_ast + team_stl + team_blk - team_tov - team_pf)
    
    opp_score = (opp_pts + opp_fgm + opp_ftm - opp_fga - opp_fta + 
                 opp_reb + opp_ast + opp_stl + opp_blk - opp_tov - opp_pf)
    
    total = team_score + opp_score
    return safe_div(team_score, total) * 100


def write_team_advanced_to_supabase(team_id, updated_fields):
    """
    Write advanced stats to team_stats table in Supabase
    """
    try:
        result = supabase.table("team_stats").update(updated_fields).eq("id", team_id).execute()
        return result
    except Exception as e:
        print(f"Error writing advanced stats for team_id {team_id}: {e}")
        return None


def fetch_team_stats_for_league(league_id):
    """
    Fetch all team_stats rows for a given league
    """
    try:
        result = supabase.table("team_stats").select("*").eq("league_id", league_id).execute()
        return result.data if result.data else []
    except Exception as e:
        print(f"Error fetching team stats for league {league_id}: {e}")
        return []


def compute_team_advanced(team_rows):
    """
    Main function to compute all advanced team metrics
    
    For each team row:
    1. Find opponent by matching game_key
    2. Calculate all advanced metrics
    3. Write back to Supabase
    
    Args:
        team_rows: List of team_stats rows from Supabase
    
    Returns:
        Number of teams processed
    """
    processed = 0
    
    # Create a lookup dict by game_key for fast opponent matching
    game_dict = {}
    for row in team_rows:
        game_key = row.get("game_key")
        if not game_key:
            continue
        if game_key not in game_dict:
            game_dict[game_key] = []
        game_dict[game_key].append(row)
    
    # Process each team
    for game_key, teams in game_dict.items():
        if len(teams) != 2:
            # Skip games without exactly 2 teams
            continue
        
        team_a, team_b = teams[0], teams[1]
        
        # Process team A vs team B
        process_team_vs_opponent(team_a, team_b)
        
        # Process team B vs team A
        process_team_vs_opponent(team_b, team_a)
        
        processed += 2
    
    return processed


def process_team_vs_opponent(team, opp):
    """
    Calculate and write all advanced stats for a team against their opponent
    """
    # Basic metrics
    team_poss = calculate_possessions(team)
    opp_poss = calculate_opp_possessions(opp)
    
    team_points = team.get("tot_spoints", 0) or 0
    opp_points = opp.get("tot_spoints", 0) or 0
    
    off_rating = calculate_off_rating(team_points, team_poss)
    def_rating = calculate_def_rating(opp_points, opp_poss)
    net_rating = calculate_net_rating(off_rating, def_rating)
    pace = calculate_pace(team_poss, opp_poss)
    
    # Efficiency metrics
    efg = calculate_efg(team)
    ts = calculate_ts(team)
    three_pt_rate = calculate_three_point_rate(team)
    
    # Rebounding
    reb_pcts = calculate_rebound_percentages(team, opp)
    
    # Assists and turnovers
    ast_pct = calculate_ast_percent(team)
    ast_to = calculate_ast_to_ratio(team)
    tov_pcts = calculate_turnover_percentages(team, opp, team_poss, opp_poss)
    
    # Scoring distribution
    scoring = calculate_scoring_distribution(team)
    
    # Four factors
    four_factors = calculate_four_factors(team, opp, team_poss, opp_poss)
    
    # PIE
    pie = calculate_pie(team, opp)
    
    # Opponent stats
    opp_fgm = opp.get("tot_sfieldgoalsmade", 0) or 0
    opp_fga = opp.get("tot_sfieldgoalsattempted", 0) or 0
    opp_3pm = opp.get("tot_sthreepointersmade", 0) or 0
    opp_turnovers = opp.get("tot_sturnovers", 0) or 0
    
    # Combine all fields
    updated_fields = {
        "possessions": team_poss,
        "opp_possessions": opp_poss,
        "off_rating": off_rating,
        "def_rating": def_rating,
        "net_rating": net_rating,
        "pace": pace,
        "efg_percent": efg,
        "ts_percent": ts,
        "three_point_rate": three_pt_rate,
        "ft_rate": four_factors["ft_rate"],
        "tov_percent": tov_pcts["tov_percent"],
        "opp_tov_percent": tov_pcts["opp_tov_percent"],
        "oreb_percent": reb_pcts["oreb_percent"],
        "dreb_percent": reb_pcts["dreb_percent"],
        "reb_percent": reb_pcts["reb_percent"],
        "opp_oreb_percent": reb_pcts["opp_oreb_percent"],
        "ast_percent": ast_pct,
        "ast_to_ratio": ast_to,
        "pie": pie,
        "opp_efg_percent": four_factors["opp_efg_percent"],
        "opp_ft_rate": four_factors["opp_ft_rate"],
        "fga_percent_2pt": safe_div((team.get("tot_stwopointersmade", 0) or 0) + (team.get("tot_stwopointersmade", 0) or 0), team.get("tot_sfieldgoalsattempted", 0) or 1) * 100,
        "fga_percent_3pt": safe_div(team.get("tot_sthreepointersattempted", 0) or 0, team.get("tot_sfieldgoalsattempted", 0) or 1) * 100,
        "pts_percent_2pt": scoring["pts_percent_2pt"],
        "pts_percent_3pt": scoring["pts_percent_3pt"],
        "pts_percent_pitp": scoring["pts_percent_pitp"],
        "pts_percent_fastbreak": scoring["pts_percent_fastbreak"],
        "pts_percent_second_chance": scoring["pts_percent_second_chance"],
        "pts_percent_off_turnovers": scoring["pts_percent_off_turnovers"],
        "pts_percent_ft": scoring["pts_percent_ft"],
        "opp_fgm": opp_fgm,
        "opp_fga": opp_fga,
        "opp_3pm": opp_3pm,
        "opp_points": opp_points,
        "opp_turnovers": opp_turnovers
    }
    
    # Write to database
    team_id = team.get("id")
    if team_id:
        write_team_advanced_to_supabase(team_id, updated_fields)
