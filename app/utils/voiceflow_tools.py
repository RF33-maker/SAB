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

