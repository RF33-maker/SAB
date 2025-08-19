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

def analyze_trending(player_name: str, records: List[dict]) -> str:
    """
    Analyze player trending based on recent games.
    Compare latest 2 games vs previous 2-3 games for key stats.
    """
    if len(records) < 3:
        return ""

    # Key stats to analyze for trending
    key_stats = ["points", "field_goal_percent", "three_pt_percent", "rebounds_total", "assists", "plus_minus"]

    # Split games: recent (latest 2) vs older (next 2-3)
    recent_games = records[:2]
    older_games = records[2:min(5, len(records))]

    trends = []
    significant_changes = []

    for stat in key_stats:
        # Calculate averages for both periods
        recent_values = [float(g.get(stat, 0)) for g in recent_games if g.get(stat) is not None]
        older_values = [float(g.get(stat, 0)) for g in older_games if g.get(stat) is not None]

        if not recent_values or not older_values:
            continue

        recent_avg = sum(recent_values) / len(recent_values)
        older_avg = sum(older_values) / len(older_values)

        # Calculate percentage change
        if older_avg != 0:
            change_pct = ((recent_avg - older_avg) / abs(older_avg)) * 100
        else:
            change_pct = 0

        # Determine if change is significant (>15% change or >3 point difference for counting stats)
        is_significant = False
        if stat in ["points", "rebounds_total", "assists"]:
            is_significant = abs(recent_avg - older_avg) >= 2.0
        elif stat in ["field_goal_percent", "three_pt_percent"]:
            is_significant = abs(change_pct) >= 10
        elif stat == "plus_minus":
            is_significant = abs(recent_avg - older_avg) >= 5.0

        if is_significant:
            direction = "up" if change_pct > 0 else "down"
            stat_display = stat.replace("_", " ").replace("percent", "%").title()

            if stat in ["points", "rebounds_total", "assists"]:
                significant_changes.append(f"{stat_display}: {direction} from {older_avg:.1f} to {recent_avg:.1f}")
            elif stat in ["field_goal_percent", "three_pt_percent"]:
                significant_changes.append(f"{stat_display}: {direction} from {older_avg:.1f}% to {recent_avg:.1f}%")
            elif stat == "plus_minus":
                significant_changes.append(f"Impact: {direction} from {older_avg:+.1f} to {recent_avg:+.1f}")

    if not significant_changes:
        return f"{player_name} has been relatively consistent across recent games"

    # Categorize overall trend
    positive_changes = sum(1 for change in significant_changes if "up from" in change)
    negative_changes = sum(1 for change in significant_changes if "down from" in change)

    if positive_changes > negative_changes:
        trend_direction = "trending upward"
        trend_emoji = "📈"
    elif negative_changes > positive_changes:
        trend_direction = "trending downward" 
        trend_emoji = "📉"
    else:
        trend_direction = "showing mixed trends"
        trend_emoji = "🔄"

    # Build response
    if len(significant_changes) <= 2:
        changes_text = " and ".join(significant_changes)
    else:
        changes_text = ", ".join(significant_changes[:-1]) + f", and {significant_changes[-1]}"

    return f"{trend_emoji} {player_name} is {trend_direction}: {changes_text}"

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
    format_mode: Optional[str] = None,
    league_id: Optional[str] = None,
    trending_analysis: Optional[bool] = True  # Add trending analysis by default
):

    global last_player_name

    # Only use cached player name if explicitly empty string or None AND no player name in user message
    if player_name == "":
        player_name = last_player_name
    elif not player_name:
        # Try to extract player name from user message before using cached name
        if user_message:
            # Look for common name patterns in the message
            import re
            # Match patterns like "How is [Player Name] doing?" or "[Player Name]'s stats"
            name_patterns = [
                r"(?:How is|What about|Tell me about|Show me)\s+([A-Z][a-z]+\s+[A-Z][a-z]+(?:\s*\([A-Z]\))?)",
                r"([A-Z][a-z]+\s+[A-Z][a-z]+(?:\s*\([A-Z]\))?)\s*(?:'s|is|has|scored|shooting)",
                r"([A-Z][a-z]+\s+[A-Z][a-z]+(?:\s*\([A-Z]\))?)\s+(?:doing|performing|stats)"
            ]
            
            for pattern in name_patterns:
                match = re.search(pattern, user_message, re.IGNORECASE)
                if match:
                    extracted_name = match.group(1).strip()
                    print(f"🎯 Extracted player name from message: '{extracted_name}'")
                    player_name = extracted_name
                    break
            
            # If no name found in message, use cached name
            if not player_name:
                player_name = last_player_name
        else:
            player_name = last_player_name

    # Update last player name if we have a valid one
    if player_name:
        last_player_name = player_name

    print(f"🔍 Final player_name being used: '{player_name}'")
    print(f"📝 Original user_message: '{user_message}'")

    # Infer mode from natural language if not provided
    if not mode and user_message:
        msg = user_message.lower()
        print(f"🧠 Inferring mode from message: '{user_message}'")
        if any(kw in msg for kw in ["average", "per game", "typically", "on average", "averages"]):
            mode = "average"
        elif any(kw in msg for kw in ["total", "in total", "overall", "combined"]):
            mode = "total"
        elif any(kw in msg for kw in ["last game", "most recent", "latest", "previous match", "yesterday"]):
            mode = "latest"
        else:
            mode = "average"  # Default to average instead of latest
        print(f"🎯 Selected mode: {mode}")

    if not player_name:
        return "⚠️ No player name provided."


    # ⛏ Fetch from Supabase
    records = fetch_player_records(player_name, league_id=league_id)
    print(f"📦 Records retrieved for {player_name} (league: {league_id}):", records)

    if not records:
        # Fallback to players table for basic info
        try:
            from app.utils.chat_data import supabase
            fallback_query = supabase.table("players").select("*").ilike("name", f"%{player_name}%")
            if league_id:
                fallback_query = fallback_query.eq("league_id", league_id)
            fallback_response = fallback_query.execute()

            if fallback_response.data:
                player_info = fallback_response.data[0]
                return f"📋 Found {player_name} in the system:\n" + \
                       f"Team: {player_info.get('team', 'N/A')}\n" + \
                       f"Position: {player_info.get('position', 'N/A')}\n" + \
                       f"Jersey #: {player_info.get('number', 'N/A')}\n" + \
                       f"Note: No game stats available yet."
        except Exception as e:
            print(f"⚠️ Fallback query failed: {e}")

        return f"❌ No records found for {player_name}."

    # Only filter to latest record if specifically requested
    if mode == "latest":
        records = [records[0]]

    # ✅ Default to key stats if no stat(s) provided
    if not stat_list:
        if stat:
            stat_list = [stat]
        else:
            # Only use essential basketball stats, exclude metadata
            essential_stats = [
                "points", "rebounds_total", "assists", "steals", "blocks", "turnovers",
                "field_goals_made", "field_goals_attempted", "field_goal_percent",
                "three_pt_made", "three_pt_attempted", "three_pt_percent",
                "free_throws_made", "free_throws_attempted", "free_throw_percent",
                "plus_minus", "minutes_played"
            ]
            record = records[0]
            stat_list = [
                key for key in essential_stats
                if key in record and isinstance(record.get(key), (int, float))
            ]
            print(f"🧾 Auto-generated stat_list for {player_name}: {stat_list}")


    results = []

    for raw_stat in stat_list or []:
        stat_key = normalize_stat(raw_stat)
        print(f"🧪 Processing stat: {raw_stat} → {stat_key}")

        try:
            # Handle team info request
            if stat_key == "team" or raw_stat.lower() in ["team", "who does he play for", "what team"]:
                if records:
                    team_name = records[0].get("team", "Unknown Team")
                    results.append(f"🏀 {player_name} plays for {team_name}.")
                else:
                    results.append(f"❌ No team information found for {player_name}.")
                continue

            # Handle percentage stats
            elif stat_key in PERCENTAGE_STATS:
                makes_key, atts_key = PERCENTAGE_STATS[stat_key]
                makes = [r.get(makes_key, 0) for r in records]
                atts = [r.get(atts_key, 0) for r in records]
                total_makes = sum([float(m) for m in makes if m is not None])
                total_atts = sum([float(a) for a in atts if a is not None])

                if total_atts == 0:
                    results.append(f"📉 No valid data to calculate {stat_key.replace('_', ' ').title()}.")
                    continue

                if mode == "average":
                    pct = round((total_makes / total_atts) * 100, 2)
                    avg_attempts = round(total_atts / len(records), 2)
                    games_count = len(records)
                    results.append(f"🎯 {player_name} averages {pct}% {stat_key.replace('_', ' ').title()} on {avg_attempts} attempts/game ({games_count} games).")
                elif mode == "total":
                    pct = round((total_makes / total_atts) * 100, 2)
                    results.append(f"🎯 {player_name}'s overall {stat_key.replace('_', ' ').title()} is {pct}% ({total_makes}/{total_atts}).")
                elif mode == "latest":
                    latest_record = records[0]
                    latest_makes = latest_record.get(makes_key, 0)
                    latest_atts = latest_record.get(atts_key, 0)
                    if latest_atts > 0:
                        latest_pct = round((latest_makes / latest_atts) * 100, 2)
                        results.append(f"🎯 In the latest game, {player_name} shot {latest_pct}% {stat_key.replace('_', ' ').title()} ({latest_makes}/{latest_atts}).")
                    else:
                        results.append(f"📉 {player_name} had no {stat_key.replace('_', ' ').title()} attempts in the latest game.")

                if mode == "average":
                    pct = round((total_makes / total_atts) * 100, 2)
                    avg_attempts = round(total_atts / len(records), 2)
                    games_count = len(records)
                    results.append(f"🎯 {player_name} averages {pct}% {stat_key.replace('_', ' ').title()} on {avg_attempts} attempts/game ({games_count} games).")
                elif mode == "total":
                    pct = round((total_makes / total_atts) * 100, 2)
                    results.append(f"🎯 {player_name}'s overall {stat_key.replace('_', ' ').title()} is {pct}% ({total_makes}/{total_atts}).")
                elif mode == "latest":
                    latest_record = records[0]
                    latest_makes = latest_record.get(makes_key, 0)
                    latest_atts = latest_record.get(atts_key, 0)
                    if latest_atts > 0:
                        latest_pct = round((latest_makes / latest_atts) * 100, 2)
                        results.append(f"🎯 In the latest game, {player_name} shot {latest_pct}% {stat_key.replace('_', ' ').title()} ({latest_makes}/{latest_atts}).")
                    else:
                        results.append(f"📉 {player_name} had no {stat_key.replace('_', ' ').title()} attempts in the latest game.")

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
                    games_count = len(values)
                    results.append(f"📊 {player_name} averages {stat_val} {stat_key.replace('_', ' ')} per game ({games_count} games).")
                elif mode == "total":
                    stat_val = round(sum(values), 2)
                    games_count = len(values)
                    results.append(f"📈 {player_name} has a total of {stat_val} {stat_key.replace('_', ' ')} ({games_count} games).")
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
        # Pull values from records[0] for latest game
        record = records[0]

        pts = record.get("points", 0)
        fg_made = record.get("field_goals_made", 0)
        fg_att = record.get("field_goals_attempted", 0)
        fg_pct = record.get("field_goal_percent", 0)
        rebounds = record.get("rebounds_total", 0)
        assists = record.get("assists", 0)
        turnovers = record.get("turnovers", 0)
        plus_minus = record.get("plus_minus", 0)
        three_made = record.get("three_pt_made", 0)
        three_att = record.get("three_pt_attempted", 0)
        steals = record.get("steals", 0)
        blocks = record.get("blocks", 0)

        # Build conversational response focusing on key areas
        response_parts = []

        # Scoring
        # Calculate actual shooting percentage from makes/attempts
        actual_fg_pct = (fg_made / fg_att * 100) if fg_att > 0 else 0

        if actual_fg_pct > 50:
            shooting_note = "shot efficiently"
        elif actual_fg_pct > 40:
            shooting_note = "shot decently"
        else:
            shooting_note = "struggled from the field"

        response_parts.append(f"🏀 {player_name} scored {pts} points on {fg_made}/{fg_att} shooting ({actual_fg_pct:.1f}%) - {shooting_note}")

        # Three-point shooting if relevant
        if three_att > 0:
            three_pct = (three_made / three_att) * 100
            response_parts.append(f"🎯 Hit {three_made}/{three_att} from three ({three_pct:.1f}%)")

        # Playmaking/Ball handling
        if assists > 0 or turnovers > 0:
            ast_to_ratio = assists / turnovers if turnovers > 0 else assists
            if ast_to_ratio >= 2:
                playmaking_note = "great ball security"
            elif ast_to_ratio >= 1:
                playmaking_note = "solid playmaking"
            else:
                playmaking_note = "needs to protect the ball better"
            response_parts.append(f"🎯 Playmaking: {assists} assists, {turnovers} turnovers - {playmaking_note}")

        # Rebounding and Defense
        defense_stats = []
        if rebounds > 0:
            defense_stats.append(f"{rebounds} rebounds")
        if steals > 0:
            defense_stats.append(f"{steals} steals")
        if blocks > 0:
            defense_stats.append(f"{blocks} blocks")

        if defense_stats:
            response_parts.append(f"🛡️ Defense: {', '.join(defense_stats)}")

        # Overall impact
        if plus_minus > 5:
            impact = "strong positive impact"
        elif plus_minus > 0:
            impact = "positive contribution"
        elif plus_minus > -5:
            impact = "neutral impact"
        else:
            impact = "struggled to impact winning"

        response_parts.append(f"📊 Impact: {plus_minus:+d} plus/minus - {impact}")

        # Follow-up suggestions
        follow_ups = [
            "• Want season averages?",
            "• Compare to other games?", 
            "• See team performance?",
            "• Check shooting trends?"
        ]

        output = f"📈 **{player_name}** - Latest Game:\n\n" + "\n".join(response_parts)
        output += f"\n\n💬 **What's next?**\n" + "\n".join(follow_ups)

        return output, records


    # Add trending analysis if we have enough games and it's requested
    if trending_analysis and len(records) >= 3:
        trending_insights = analyze_trending(player_name, records)
        if trending_insights:
            results.append(f"\n📈 **Trending Analysis**: {trending_insights}")

    # Always provide intelligent conversational analysis when we have multiple stats
    if len(stat_list) > 3 or not stat:  # Multiple stats or general query
        record = records[0]
        pts = record.get("points", 0)
        fg_made = record.get("field_goals_made", 0)
        fg_att = record.get("field_goals_attempted", 0)
        rebounds = record.get("rebounds_total", 0)
        assists = record.get("assists", 0)
        turnovers = record.get("turnovers", 0)
        plus_minus = record.get("plus_minus", 0)
        three_made = record.get("three_pt_made", 0)
        three_att = record.get("three_pt_attempted", 0)
        minutes = record.get("minutes_played", "0:00")

        # Calculate shooting efficiency
        actual_fg_pct = (fg_made / fg_att * 100) if fg_att > 0 else 0
        three_pct = (three_made / three_att * 100) if three_att > 0 else 0

        # Determine overall performance level
        performance_indicators = []
        
        # Scoring assessment
        if pts >= 20:
            performance_indicators.append("strong")
        elif pts >= 12:
            performance_indicators.append("solid")
        elif pts >= 8:
            performance_indicators.append("decent")
        else:
            performance_indicators.append("struggling")

        # Efficiency assessment
        if actual_fg_pct >= 50:
            performance_indicators.append("efficient")
        elif actual_fg_pct >= 40:
            performance_indicators.append("acceptable")
        else:
            performance_indicators.append("inefficient")

        # Plus/minus assessment
        if plus_minus >= 10:
            impact_level = "excellent"
        elif plus_minus >= 5:
            impact_level = "strong"
        elif plus_minus >= 0:
            impact_level = "positive"
        elif plus_minus >= -5:
            impact_level = "mixed"
        else:
            impact_level = "poor"

        # Get trending analysis
        trend_direction = ""
        if trending_analysis and len(records) >= 3:
            trending_insights = analyze_trending(player_name, records)
            if "trending upward" in trending_insights:
                trend_direction = "on an upward trend"
            elif "trending downward" in trending_insights:
                trend_direction = "in a downward trend"
            else:
                trend_direction = "showing consistent form"

        # Build intelligent response
        response_parts = []

        # Overall assessment
        if "strong" in performance_indicators and impact_level in ["excellent", "strong"]:
            overall_assessment = "performing excellently"
        elif "solid" in performance_indicators and impact_level in ["strong", "positive"]:
            overall_assessment = "performing well"
        elif "decent" in performance_indicators:
            overall_assessment = "performing okay"
        else:
            overall_assessment = "struggling"

        # Start with overall assessment and trend
        if trend_direction:
            response_parts.append(f"{player_name} is {overall_assessment} and {trend_direction}.")
        else:
            response_parts.append(f"{player_name} is {overall_assessment}.")

        # Latest game performance
        game_summary = f"In his latest game, he scored {pts} points"
        
        if fg_att > 0:
            game_summary += f" on {fg_made}/{fg_att} shooting ({actual_fg_pct:.1f}%)"
        
        if three_att > 0:
            game_summary += f", hit {three_made}/{three_att} from three ({three_pct:.1f}%)"
            
        game_summary += f", grabbed {rebounds} rebounds, and dished out {assists} assists"
        
        if turnovers > 0:
            game_summary += f" with {turnovers} turnovers"
            
        game_summary += f". His impact was {impact_level} with a {plus_minus:+d} plus/minus."

        response_parts.append(game_summary)

        # Add context about what the numbers mean
        context_notes = []
        
        if actual_fg_pct < 35:
            context_notes.append("He needs to improve his shot selection and efficiency")
        elif actual_fg_pct > 55:
            context_notes.append("His shooting efficiency is excellent")
            
        if three_att >= 5 and three_pct < 30:
            context_notes.append("his three-point shooting needs work")
        elif three_att >= 3 and three_pct > 40:
            context_notes.append("his three-point shooting is on point")
            
        if turnovers > assists and assists > 0:
            context_notes.append("he needs to take better care of the ball")
        elif assists > turnovers * 2:
            context_notes.append("he's showing good court vision and ball security")

        if plus_minus < -10:
            context_notes.append("the team struggled when he was on the court")
        elif plus_minus > 10:
            context_notes.append("the team performed much better with him on the floor")

        if context_notes:
            if len(context_notes) == 1:
                response_parts.append(f"Analysis: {context_notes[0].capitalize()}.")
            else:
                response_parts.append(f"Analysis: {context_notes[0].capitalize()}, and {', '.join(context_notes[1:])}.")

        return " ".join(response_parts)
    else:
        # For specific stat requests, return the normal detailed response
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


async def get_player_trending(
    player_name: str,
    league_id: Optional[str] = None,
    games_to_analyze: Optional[int] = 5
):
    """
    Get detailed trending analysis for a specific player.

    Args:
        player_name: Name of the player
        league_id: Optional league filter
        games_to_analyze: Number of recent games to analyze (default 5)
    """

    try:
        # Get player records
        records = fetch_player_records(player_name, league_id=league_id)

        if not records:
            return f"❌ No records found for {player_name}."

        if len(records) < 3:
            return f"⚠️ Need at least 3 games for trending analysis. {player_name} has {len(records)} game(s)."

        # Limit to requested number of games
        records = records[:games_to_analyze]

        # Get comprehensive trending analysis
        trending_result = analyze_trending(player_name, records)

        if not trending_result:
            return f"📊 {player_name} shows consistent performance across {len(records)} recent games."

        # Add game-by-game breakdown
        game_breakdown = []
        for i, game in enumerate(records[:3], 1):
            pts = game.get("points", 0)
            fg_pct = game.get("field_goal_percent", 0)
            reb = game.get("rebounds_total", 0)
            ast = game.get("assists", 0)
            date = game.get("game_date", "Unknown")

            game_breakdown.append(f"Game {i} ({date}): {pts}pts, {reb}reb, {ast}ast ({fg_pct:.1f}% FG)")

        detailed_response = [
            f"📈 **Trending Analysis for {player_name}**",
            "",
            trending_result,
            "",
            f"📋 **Recent Game Breakdown**:",
            *game_breakdown
        ]

        return "\n".join(detailed_response)

    except Exception as e:
        print(f"❌ Error in get_player_trending: {str(e)}")
        return f"⚠️ Error analyzing trends for {player_name}: {str(e)}"

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