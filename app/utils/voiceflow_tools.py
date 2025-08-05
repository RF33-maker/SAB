    # app/utils/voiceflow_tools.py

import os
import requests
import asyncio
from datetime import datetime
from typing import Optional, List
from decimal import Decimal
from app.utils.chat_data import fetch_player_records


STAT_ALIASES = {
        # Totals
        "pts":            "points",
        "points":         "points",
        "reb":            "rebounds_total",
        "rebounds":       "rebounds_total",
        "ast":            "assists",
        "assists":        "assists",
        "stl":            "steals",
        "steals":         "steals",
        "blk":            "blocks",
        "blocks":         "blocks",
        "plus/minus":     "plus_minus",
        "plus-minus":     "plus_minus",
        "to":             "turnovers",
        "turnovers":      "turnovers",
        "pf":             "personal_fouls",    
        "fouls":          "personal_fouls",
        "fouls drawn":    "fouls_drawn",
        "ast/to":         "assist_turnover_ratio",
        "ast/to ratio":   "assist_turnover_ratio",
        "ast to ratio":   "assist_turnover_ratio",
        "ast:to":         "assist_turnover_ratio",
        "ast-to":         "assist_turnover_ratio",
        "ast-to ratio":   "assist_turnover_ratio",
        "ast:to ratio":   "assist_turnover_ratio",
        "ast-to-ratio":   "assist_turnover_ratio",
    
        # Percentages
        "fg%":            "field_goal_percent",
        "field goal %":   "field_goal_percent",
        "field goal pct": "field_goal_percent",    
        "field goal percentage": "field_goal_percent",
        "efg":            "effective_fg_percent",
        "efg%":           "effective_fg_percent",
        "effective field goal %": "effective_fg_percent",
        "ts":             "true_shooting_percent",
        "true shooting":  "true_shooting_percent",
        "true shooting %": "true_shooting_percent",
        "true shooting pct": "true_shooting_percent",
        "true shooting percentage": "true_shooting_percent",
        "3pt%":           "three_pt_percent",
        "3p%":            "three_pt_percent",
        "3 point %":      "three_pt_percent",
        "3 point percentage": "three_pt_percent",
        "three point %":  "three_pt_percent",
        "three point pct": "three_pt_percent",
        "three point percentage": "three_pt_percent",
        "three point field goals percentage": "three_pt_percent",
        "three point field goal pct": "three_pt_percent",
        "three point field goal %": "three_pt_percent",
        "2pt%":           "two_pt_percent",
        "2p%":            "two_pt_percent",    
        "2 point %":      "two_pt_percent",
        "two point %":    "two_pt_percent",    
        "two point pct":  "two_pt_percent",
        "two point percentage": "two_pt_percent",
        "ft%":            "free_throw_percent",
        "free throw %":   "free_throw_percent",
        "free throws %":  "free_throw_percent",    
        "free throw pct": "free_throw_percent",
        "free throw percentage": "free_throw_percent",

        # Attempts
        "fga":            "field_goals_attempted",
        "field goals attempted": "field_goals_attempted",
        "fgm":            "field_goals_made",
        "field goals made": "field_goals_made",
        "fta":            "free_throws_attempted",
        "ftm":            "free_throws_made",
        "3pa":            "three_pt_attempted",
        "3 point attempts":      "three_pt_attempted",
        "3s per game":          "three_pt_attempted",
        "2pa":            "two_pt_attempted",    
        "2 point attempts":      "two_pt_attempted",
        "2s per game":          "two_pt_attempted"
    }

PERCENTAGE_STATS = {
    "three_pt_percent": ("three_pt_made", "three_pt_attempted"),
    "two_pt_percent": ("two_pt_made", "two_pt_attempted"),
    "field_goal_percent": ("field_goals_made", "field_goals_attempted"),
    "free_throw_percent": ("free_throws_made", "free_throws_attempted"),
}


last_player_name = None

def normalize_stat(raw: str) -> str:
    if not raw:
        return ""
    key = raw.strip().lower().replace("-", " ").replace("_", " ")
    key = key.replace("three", "3").replace("two", "2")
    return STAT_ALIASES.get(key, key.replace(" ", "_"))

async def get_player_stats(
    player_name: Optional[str] = None,
    stat: Optional[str] = None,
    stat_list: Optional[List[str]] = None,
    mode: Optional[str] = None,
    user_message: Optional[str] = None,
    format_mode: Optional[str] = None  # ✅ Add this line
):

    global last_player_name

    if not player_name:
        player_name = last_player_name
    if player_name:
        last_player_name = player_name

    # Infer mode from natural language if not provided
    if not mode and user_message:
        msg = user_message.lower()
        if any(kw in msg for kw in ["average", "per game", "typically", "on average"]):
            mode = "average"
        elif any(kw in msg for kw in ["total", "in total", "overall", "combined"]):
            mode = "total"
        elif any(kw in msg for kw in ["last game", "most recent", "latest", "previous match", "yesterday"]):
            mode = "latest"
        else:
            mode = "latest"  # fallback

        if not player_name:
            return "⚠️ No player name provided."


    # ⛏ Fetch from Supabase
    records = fetch_player_records(player_name)
    print(f"📦 Records retrieved for {player_name}:", records)

    if not records:
        return f"❌ No records found for {player_name}."

    if mode == "latest":
        records = [records[0]]

    # ✅ Default to all numeric stats if no stat(s) provided
    if not stat_list:
        if stat:
            stat_list = [stat]
        else:
            record = records[0]
            stat_list = [
                key for key, value in record.items()
                if isinstance(value, (int, float)) and key not in ("game_id", "number")
            ]
            print(f"🧾 Auto-generated stat_list for {player_name}: {stat_list}")


    results = []

    for raw_stat in stat_list or []:
        stat_key = normalize_stat(raw_stat)
        print(f"🧪 Processing stat: {raw_stat} → {stat_key}")

        try:
            # Handle percentage stats
            if stat_key in PERCENTAGE_STATS:
                makes_key, atts_key = PERCENTAGE_STATS[stat_key]
                makes = [r.get(makes_key, 0) for r in records]
                atts = [r.get(atts_key, 0) for r in records]
                total_makes = sum([float(m) for m in makes if m is not None])
                total_atts = sum([float(a) for a in atts if a is not None])

                if total_atts == 0:
                    results.append(f"📉 No valid data to calculate {stat_key.replace('_', ' ').title()}.")
                    continue

                pct = round((total_makes / total_atts) * 100, 2)
                avg_attempts = round(total_atts / len(records), 2)
                results.append(f"🎯 {player_name}'s {stat_key.replace('_', ' ').title()} is {pct}% (avg {avg_attempts} attempts/game).")

            else:
                values = [
                    float(r.get(stat_key, 0))
                    for r in records
                    if r.get(stat_key) is not None and isinstance(r.get(stat_key), (int, float, Decimal))
                ]


                if not values:
                    results.append(f"📉 No valid data for {stat_key.replace('_', ' ').title()}.")
                    continue

                if mode == "average":
                    stat_val = round(sum(values) / len(values), 2)
                    results.append(f"📊 {player_name} averages {stat_val} {stat_key.replace('_', ' ')} per game.")
                elif mode == "total":
                    stat_val = round(sum(values), 2)
                    results.append(f"📈 {player_name} has a total of {stat_val} {stat_key.replace('_', ' ')}.")
                elif mode == "latest":
                    stat_val = round(values[0], 2)
                    results.append(f"🆕 In the latest game, {player_name} recorded {stat_val} {stat_key.replace('_', ' ')}.")
        except Exception as e:
            print(f"❌ Error processing stat '{stat_key}': {str(e)}")
            results.append(f"⚠️ Error processing {stat_key.replace('_', ' ')}.")

        if not results:
            print(f"❗ No results generated for stat_list: {stat_list}")
            return f"⚠️ Stats not found or fields mismatched for {player_name}."


    if format_mode == "cleaned":

        def fmt(label, value):
            return f"- {label}: {value}"

        # Pull values from records[0] for latest game
        record = records[0]

        scoring = [
            fmt("Points", record.get("points")),
            fmt("FG", f'{record.get("field_goals_made")}/{record.get("field_goals_attempted")} ({record.get("field_goal_percent", 0):.2f}%)'),
            fmt("FT", f'{record.get("free_throws_made")}/{record.get("free_throws_attempted")} ({record.get("free_throw_percent", 0):.2f}%)')
        ]

        rebounding = [
            fmt("Total", record.get("rebounds_total")),
            fmt("Offensive", record.get("rebounds_offensive")),
            fmt("Defensive", record.get("rebounds_defensive"))
        ]

        playmaking = [
            fmt("Assists", record.get("assists")),
            fmt("Turnovers", record.get("turnovers")),
            fmt("AST/TO", record.get("assist_turnover_ratio"))
        ]

        defense = [
            fmt("Steals", record.get("steals")),
            fmt("Blocks", record.get("blocks"))
        ]

        extras = [
            fmt("Plus-Minus", record.get("plus_minus")),
            fmt("Fouls", record.get("personal_fouls")),
            fmt("Fouls Drawn", record.get("fouls_drawn")),
            fmt("True Shooting %", f'{record.get("true_shooting_percent", 0):.2f}%')
        ]

        output = (
            f"📊 {player_name} — Last Game Summary\n\n"
            f"🟠 Scoring:\n" + "\n".join(scoring) + "\n\n" +
            f"🟡 Rebounding:\n" + "\n".join(rebounding) + "\n\n" +
            f"🔵 Playmaking:\n" + "\n".join(playmaking) + "\n\n" +
            f"🛡️ Defense:\n" + "\n".join(defense) + "\n\n" +
            f"🧱 Extras:\n" + "\n".join(extras)
        )

        return output, records

    
    return " ".join(results)


async def get_top_players(
    stat: str,
    limit: Optional[int] = 5,
    mode: Optional[str] = "latest",
    user_message: Optional[str] = None
):
    """
    Get top players in a specific stat category.
    
    Args:
        stat: The stat to rank by (e.g., "points", "rebounds", "assists")
        limit: Number of top players to return (default 5)
        mode: "latest" (last game), "average" (per game), or "total" (season)
        user_message: Original user query for context
    """
    
    # Normalize the stat name
    stat_key = normalize_stat(stat)
    
    # Infer mode from user message if not provided
    if not mode and user_message:
        msg = user_message.lower()
        if any(kw in msg for kw in ["average", "per game", "typically"]):
            mode = "average"
        elif any(kw in msg for kw in ["total", "season", "overall"]):
            mode = "total"
        else:
            mode = "latest"
    
    try:
        # Get all player records from Supabase
        from app.utils.chat_data import supabase
        response = supabase.table("player_stats").select("*").order("game_date", desc=True).execute()
        
        if not response.data:
            return "❌ No player data found in database."
        
        all_records = response.data
        
        # Group records by player
        player_stats = {}
        for record in all_records:
            player_name = record.get("name")
            if not player_name:
                continue
                
            if player_name not in player_stats:
                player_stats[player_name] = []
            player_stats[player_name].append(record)
        
        # Calculate stat values for each player based on mode
        player_rankings = []
        
        for player_name, records in player_stats.items():
            if mode == "latest":
                # Use most recent game
                latest_record = records[0]  # Already sorted by date desc
                stat_value = latest_record.get(stat_key, 0)
                if stat_value is not None:
                    player_rankings.append({
                        "name": player_name,
                        "value": float(stat_value),
                        "team": latest_record.get("team", ""),
                        "game_date": latest_record.get("game_date", "")
                    })
                    
            elif mode == "average":
                # Calculate per-game average
                values = [float(r.get(stat_key, 0)) for r in records if r.get(stat_key) is not None]
                if values:
                    avg_value = sum(values) / len(values)
                    player_rankings.append({
                        "name": player_name,
                        "value": round(avg_value, 2),
                        "team": records[0].get("team", ""),
                        "games": len(values)
                    })
                    
            elif mode == "total":
                # Calculate season total
                values = [float(r.get(stat_key, 0)) for r in records if r.get(stat_key) is not None]
                if values:
                    total_value = sum(values)
                    player_rankings.append({
                        "name": player_name,
                        "value": round(total_value, 2),
                        "team": records[0].get("team", ""),
                        "games": len(values)
                    })
        
        # Sort by stat value (descending)
        player_rankings.sort(key=lambda x: x["value"], reverse=True)
        
        # Limit results
        top_players = player_rankings[:limit]
        
        if not top_players:
            return f"❌ No valid data found for stat '{stat_key.replace('_', ' ')}'."
        
        # Format response
        stat_display = stat_key.replace("_", " ").title()
        mode_display = {"latest": "Latest Game", "average": "Per Game Average", "total": "Season Total"}[mode]
        
        results = [f"🏆 Top {len(top_players)} Players - {stat_display} ({mode_display}):\n"]
        
        for i, player in enumerate(top_players, 1):
            if mode == "latest":
                results.append(f"{i}. {player['name']} ({player['team']}) - {player['value']} ({player['game_date']})")
            else:
                results.append(f"{i}. {player['name']} ({player['team']}) - {player['value']} ({player['games']} games)")
        
        return "\n".join(results)
        
    except Exception as e:
        print(f"❌ Error in get_top_players: {str(e)}")
        return f"⚠️ Error retrieving top players for {stat_key.replace('_', ' ')}: {str(e)}"


async def get_game_summary(
    game_date: Optional[str] = None,
    home_team: Optional[str] = None,
    away_team: Optional[str] = None,
    query_type: Optional[str] = "basic"
):
    """
    Get game summary information including scores, team stats, and key performances.
    
    Args:
        game_date: Date of the game (YYYY-MM-DD format)
        home_team: Home team name
        away_team: Away team name  
        query_type: "basic", "detailed", "quarters", or "team_comparison"
    """
    
    try:
        from app.utils.chat_data import supabase
        
        # Build query to get game data
        query = supabase.table("player_stats").select("*")
        
        if game_date:
            query = query.eq("game_date", game_date)
        if home_team:
            query = query.eq("home_team", home_team)
        if away_team:
            query = query.eq("away_team", away_team)
            
        response = query.execute()
        
        if not response.data:
            return "❌ No game data found for the specified criteria."
        
        game_records = response.data
        
        # Get basic game info
        first_record = game_records[0]
        home_team_name = first_record.get("home_team", "Home")
        away_team_name = first_record.get("away_team", "Away")
        game_date_str = first_record.get("game_date", "Unknown Date")
        
        # Separate players by team
        home_players = [r for r in game_records if r.get("team") == home_team_name]
        away_players = [r for r in game_records if r.get("team") == away_team_name]
        
        if query_type == "basic":
            # Basic game summary
            home_points = sum(r.get("points", 0) for r in home_players)
            away_points = sum(r.get("points", 0) for r in away_players)
            
            # Top scorers
            home_top_scorer = max(home_players, key=lambda x: x.get("points", 0)) if home_players else None
            away_top_scorer = max(away_players, key=lambda x: x.get("points", 0)) if away_players else None
            
            result = [
                f"🏀 Game Summary - {game_date_str}",
                f"📊 Final Score: {home_team_name} {home_points} - {away_points} {away_team_name}",
                ""
            ]
            
            if home_top_scorer:
                result.append(f"🔥 {home_team_name} Top Scorer: {home_top_scorer['name']} ({home_top_scorer.get('points', 0)} pts)")
            if away_top_scorer:
                result.append(f"🔥 {away_team_name} Top Scorer: {away_top_scorer['name']} ({away_top_scorer.get('points', 0)} pts)")
                
            return "\n".join(result)
            
        elif query_type == "team_comparison":
            # Team vs team comparison
            def team_total(players, stat):
                return sum(r.get(stat, 0) for r in players)
            
            def team_avg(players, stat):
                values = [r.get(stat, 0) for r in players if r.get(stat) is not None]
                return round(sum(values) / len(values), 1) if values else 0
            
            result = [
                f"⚔️ Team Comparison - {home_team_name} vs {away_team_name}",
                f"📅 Date: {game_date_str}",
                "",
                f"🏀 Points: {team_total(home_players, 'points')} - {team_total(away_players, 'points')}",
                f"🔄 Rebounds: {team_total(home_players, 'rebounds_total')} - {team_total(away_players, 'rebounds_total')}",
                f"🎯 Assists: {team_total(home_players, 'assists')} - {team_total(away_players, 'assists')}",
                f"❌ Turnovers: {team_total(home_players, 'turnovers')} - {team_total(away_players, 'turnovers')}",
                f"🛡️ Steals: {team_total(home_players, 'steals')} - {team_total(away_players, 'steals')}",
                f"🚫 Blocks: {team_total(home_players, 'blocks')} - {team_total(away_players, 'blocks')}",
                "",
                f"📈 Field Goal %: {team_avg(home_players, 'field_goal_percent'):.1f}% - {team_avg(away_players, 'field_goal_percent'):.1f}%",
                f"🎯 3-Point %: {team_avg(home_players, 'three_pt_percent'):.1f}% - {team_avg(away_players, 'three_pt_percent'):.1f}%",
                f"🆓 Free Throw %: {team_avg(home_players, 'free_throw_percent'):.1f}% - {team_avg(away_players, 'free_throw_percent'):.1f}%"
            ]
            
            return "\n".join(result)
            
        elif query_type == "detailed":
            # Detailed game breakdown
            home_points = sum(r.get("points", 0) for r in home_players)
            away_points = sum(r.get("points", 0) for r in away_players)
            
            # Get top performers in various categories
            all_players = home_players + away_players
            top_scorer = max(all_players, key=lambda x: x.get("points", 0))
            top_rebounder = max(all_players, key=lambda x: x.get("rebounds_total", 0))
            top_assist = max(all_players, key=lambda x: x.get("assists", 0))
            
            result = [
                f"📊 Detailed Game Report - {game_date_str}",
                f"🏀 Final Score: {home_team_name} {home_points} - {away_points} {away_team_name}",
                "",
                f"🔥 Top Scorer: {top_scorer['name']} ({top_scorer.get('points', 0)} pts, {top_scorer['team']})",
                f"🏀 Top Rebounder: {top_rebounder['name']} ({top_rebounder.get('rebounds_total', 0)} reb, {top_rebounder['team']})",
                f"🎯 Most Assists: {top_assist['name']} ({top_assist.get('assists', 0)} ast, {top_assist['team']})",
                "",
                f"👥 Players Played: {len(home_players)} ({home_team_name}), {len(away_players)} ({away_team_name})"
            ]
            
            return "\n".join(result)
            
        return "⚠️ Invalid query type specified."
        
    except Exception as e:
        print(f"❌ Error in get_game_summary: {str(e)}")
        return f"⚠️ Error retrieving game summary: {str(e)}"


async def get_team_analysis(
    team_name: str,
    analysis_type: Optional[str] = "roster",
    game_date: Optional[str] = None
):
    """
    Analyze team performance, roster, or specific aspects like bench production.
    
    Args:
        team_name: Name of the team to analyze
        analysis_type: "roster", "bench", "starters", "efficiency", "shooting_splits"
        game_date: Optional specific game date
    """
    
    try:
        from app.utils.chat_data import supabase
        
        # Get team data
        query = supabase.table("player_stats").select("*").eq("team", team_name)
        
        if game_date:
            query = query.eq("game_date", game_date)
        else:
            # Get most recent game
            query = query.order("game_date", desc=True).limit(15)  # Assume max 15 players
            
        response = query.execute()
        
        if not response.data:
            return f"❌ No data found for team '{team_name}'."
        
        team_players = response.data
        game_date_display = team_players[0].get("game_date", "Unknown Date")
        
        if analysis_type == "roster":
            # Full roster performance
            team_players.sort(key=lambda x: x.get("points", 0), reverse=True)
            
            result = [
                f"📋 {team_name} Roster Performance - {game_date_display}",
                ""
            ]
            
            for i, player in enumerate(team_players, 1):
                mins = player.get("minutes", 0)
                pts = player.get("points", 0)
                reb = player.get("rebounds_total", 0)
                ast = player.get("assists", 0)
                
                result.append(f"{i:2d}. {player['name']:20s} - {pts:2d} pts, {reb:2d} reb, {ast:2d} ast ({mins} min)")
            
            return "\n".join(result)
            
        elif analysis_type == "efficiency":
            # Calculate efficiency ratings and rank players
            def calculate_efficiency(player):
                pts = player.get("points", 0)
                reb = player.get("rebounds_total", 0)
                ast = player.get("assists", 0)
                stl = player.get("steals", 0)
                blk = player.get("blocks", 0)
                to = player.get("turnovers", 0)
                fgm = player.get("field_goals_made", 0)
                fga = player.get("field_goals_attempted", 0)
                ftm = player.get("free_throws_made", 0)
                fta = player.get("free_throws_attempted", 0)
                
                # Simple efficiency formula
                efficiency = pts + reb + ast + stl + blk - to - (fga - fgm) - (fta - ftm)
                return efficiency
            
            # Add efficiency to each player and sort
            for player in team_players:
                player['efficiency'] = calculate_efficiency(player)
            
            team_players.sort(key=lambda x: x.get('efficiency', 0), reverse=True)
            
            result = [
                f"⚡ {team_name} Efficiency Rankings - {game_date_display}",
                ""
            ]
            
            for i, player in enumerate(team_players, 1):
                eff = player.get('efficiency', 0)
                mins = player.get("minutes", 0)
                result.append(f"{i:2d}. {player['name']:20s} - {eff:+3.0f} efficiency ({mins} min)")
            
            return "\n".join(result)
            
        elif analysis_type == "shooting_splits":
            # Team shooting analysis
            def safe_percentage(made, attempted):
                return (made / attempted * 100) if attempted > 0 else 0
            
            total_fg_made = sum(p.get("field_goals_made", 0) for p in team_players)
            total_fg_att = sum(p.get("field_goals_attempted", 0) for p in team_players)
            total_3p_made = sum(p.get("three_pt_made", 0) for p in team_players)
            total_3p_att = sum(p.get("three_pt_attempted", 0) for p in team_players)
            total_ft_made = sum(p.get("free_throws_made", 0) for p in team_players)
            total_ft_att = sum(p.get("free_throws_attempted", 0) for p in team_players)
            
            fg_pct = safe_percentage(total_fg_made, total_fg_att)
            three_pct = safe_percentage(total_3p_made, total_3p_att)
            ft_pct = safe_percentage(total_ft_made, total_ft_att)
            
            result = [
                f"🎯 {team_name} Shooting Splits - {game_date_display}",
                "",
                f"🏀 Field Goals: {total_fg_made}/{total_fg_att} ({fg_pct:.1f}%)",
                f"🎯 Three-Pointers: {total_3p_made}/{total_3p_att} ({three_pct:.1f}%)",
                f"🆓 Free Throws: {total_ft_made}/{total_ft_att} ({ft_pct:.1f}%)",
                "",
                f"📊 Individual Shooting (min 3 FGA):"
            ]
            
            # Show individual shooting for players with significant attempts
            shooters = [p for p in team_players if p.get("field_goals_attempted", 0) >= 3]
            shooters.sort(key=lambda x: x.get("field_goal_percent", 0), reverse=True)
            
            for player in shooters:
                name = player['name']
                fg_made = player.get("field_goals_made", 0)
                fg_att = player.get("field_goals_attempted", 0)
                fg_pct = player.get("field_goal_percent", 0)
                
                result.append(f"  {name:20s} - {fg_made}/{fg_att} ({fg_pct:.1f}%)")
            
            return "\n".join(result)
            
        return f"⚠️ Invalid analysis type '{analysis_type}'. Use: roster, efficiency, shooting_splits"
        
    except Exception as e:
        print(f"❌ Error in get_team_analysis: {str(e)}")
        return f"⚠️ Error analyzing team: {str(e)}"


async def get_advanced_insights(
    insight_type: str,
    limit: Optional[int] = 5,
    team_filter: Optional[str] = None,
    game_date: Optional[str] = None
):
    """
    Generate advanced basketball insights like top performers, starting 5 recommendations, etc.
    
    Args:
        insight_type: "top_performers", "starting_five", "game_impact", "clutch_players"
        limit: Number of results to return
        team_filter: Filter by specific team
        game_date: Filter by specific game date
    """
    
    try:
        from app.utils.chat_data import supabase
        
        # Build base query
        query = supabase.table("player_stats").select("*")
        
        if team_filter:
            query = query.eq("team", team_filter)
        if game_date:
            query = query.eq("game_date", game_date)
        else:
            query = query.order("game_date", desc=True).limit(50)  # Recent games
            
        response = query.execute()
        
        if not response.data:
            return "❌ No data found for analysis."
        
        players = response.data
        
        if insight_type == "top_performers":
            # Multi-criteria performance ranking
            def performance_score(player):
                pts = player.get("points", 0)
                reb = player.get("rebounds_total", 0)
                ast = player.get("assists", 0)
                stl = player.get("steals", 0)
                blk = player.get("blocks", 0)
                to = player.get("turnovers", 0)
                fg_pct = player.get("field_goal_percent", 0)
                
                # Weighted performance score
                score = (pts * 1.0) + (reb * 0.8) + (ast * 1.2) + (stl * 1.5) + (blk * 1.5) - (to * 1.0) + (fg_pct * 0.3)
                return score
            
            # Sort by performance score
            for player in players:
                player['perf_score'] = performance_score(player)
            
            players.sort(key=lambda x: x.get('perf_score', 0), reverse=True)
            top_performers = players[:limit]
            
            result = [f"🌟 Top {limit} Overall Performers:\n"]
            
            for i, player in enumerate(top_performers, 1):
                pts = player.get("points", 0)
                reb = player.get("rebounds_total", 0)
                ast = player.get("assists", 0)
                team = player.get("team", "")
                score = player.get('perf_score', 0)
                
                result.append(f"{i}. {player['name']} ({team}) - {pts}pts/{reb}reb/{ast}ast (Score: {score:.1f})")
            
            return "\n".join(result)
            
        elif insight_type == "starting_five":
            # Recommend starting 5 based on performance
            if not team_filter:
                return "⚠️ Please specify a team for starting 5 recommendation."
            
            team_players = [p for p in players if p.get("team") == team_filter]
            
            if len(team_players) < 5:
                return f"⚠️ Not enough players found for {team_filter}."
            
            # Score players by position value (simplified)
            def starting_value(player):
                pts = player.get("points", 0)
                reb = player.get("rebounds_total", 0)
                ast = player.get("assists", 0)
                mins = player.get("minutes", 0)
                fg_pct = player.get("field_goal_percent", 0)
                
                # Value formula emphasizing minutes and efficiency
                value = (pts + reb + ast) * (mins / 40) * (fg_pct / 100 + 0.5)
                return value
            
            # Sort team by value and take top 5
            for player in team_players:
                player['starting_value'] = starting_value(player)
            
            team_players.sort(key=lambda x: x.get('starting_value', 0), reverse=True)
            starting_five = team_players[:5]
            
            result = [f"🏀 Recommended Starting 5 for {team_filter}:\n"]
            
            positions = ["PG", "SG", "SF", "PF", "C"]  # Simplified position assignment
            
            for i, player in enumerate(starting_five):
                pos = positions[i] if i < len(positions) else "F"
                pts = player.get("points", 0)
                reb = player.get("rebounds_total", 0)
                ast = player.get("assists", 0)
                mins = player.get("minutes", 0)
                
                result.append(f"{pos}: {player['name']} - {pts}pts/{reb}reb/{ast}ast ({mins}min)")
            
            return "\n".join(result)
            
        elif insight_type == "game_impact":
            # Players with highest +/- and clutch stats
            players_with_impact = [p for p in players if p.get("plus_minus") is not None]
            players_with_impact.sort(key=lambda x: x.get("plus_minus", -999), reverse=True)
            
            top_impact = players_with_impact[:limit]
            
            result = [f"💥 Highest Game Impact (+/-): \n"]
            
            for i, player in enumerate(top_impact, 1):
                plus_minus = player.get("plus_minus", 0)
                pts = player.get("points", 0)
                team = player.get("team", "")
                mins = player.get("minutes", 0)
                
                result.append(f"{i}. {player['name']} ({team}) - {plus_minus:+d} (+/-), {pts}pts in {mins}min")
            
            return "\n".join(result)
            
        return f"⚠️ Invalid insight type '{insight_type}'. Use: top_performers, starting_five, game_impact"
        
    except Exception as e:
        print(f"❌ Error in get_advanced_insights: {str(e)}")
        return f"⚠️ Error generating insights: {str(e)}"

