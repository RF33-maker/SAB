from supabase import create_client
import os

supabase = create_client(os.getenv("SUPABASE_URL"), os.getenv("SUPABASE_KEY"))

def get_league_info(league_name):
    return supabase.table("leagues").select("*").ilike("name", f"%{league_name}%").execute().data

def get_team_info(team_name):
    return supabase.table("teams").select("*").ilike("team", f"%{team_name}%").execute().data

def get_player_stats(player_name):
    return supabase.table("player_stats").select("*").ilike("name", f"%{player_name}%").execute().data

def get_player_averages(player_name):
    return supabase.table("player_season_averages").select("*").ilike("name", f"%{player_name}%").execute().data

def get_team_stats(team_name):
    return supabase.table("team_stats").select("*").ilike("team", f"%{team_name}%").execute().data

def get_summaries(team_name):
    return supabase.table("summaries").select("*").ilike("team", f"%{team_name}%").order("created_at", desc=True).limit(3).execute().data

def get_recent_games(team_name):
    return supabase.table("game_schedule").select("*").or_(
        f"home_team.ilike.%{team_name}%,away_team.ilike.%{team_name}%"
    ).order("game_date", desc=True).limit(3).execute().data
