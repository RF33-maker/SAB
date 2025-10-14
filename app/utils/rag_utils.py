"""
RAG Utilities for Swish Assistant
Provides entity detection and context building for basketball analytics queries
"""

import re
from typing import Dict, List, Optional, Tuple
from app.utils.chat_data import supabase
import logging


def detect_entities(question: str, league_id: Optional[str] = None) -> Dict:
    """
    Detect entities (player, team, league) from user question
    
    Returns dict with:
    - entity_type: 'player', 'team', 'league', or 'general'
    - entity_name: detected name if applicable
    - league_id: league context if provided
    """
    question_lower = question.lower()
    
    # Player indicators
    player_keywords = ['player', 'scored', 'points', 'rebounds', 'assists', 'stats', 'performance', 'game']
    
    # Team indicators  
    team_keywords = ['team', 'roster', 'record', 'standing', 'vs', 'against', 'played']
    
    # League indicators
    league_keywords = ['league', 'top', 'leaders', 'best', 'ranking', 'standings']
    
    # Check for player names in database (if league_id provided)
    detected_player = None
    if league_id:
        # Try to find player names from the database
        players_result = supabase.table("players").select("full_name").eq("league_id", league_id).execute()
        if players_result.data:
            for player in players_result.data:
                player_name = player.get('full_name', '')
                if player_name.lower() in question_lower:
                    detected_player = player_name
                    break
    
    # Check for team names
    detected_team = None
    if league_id:
        teams_result = supabase.table("teams").select("name").eq("league_id", league_id).execute()
        if teams_result.data:
            for team in teams_result.data:
                team_name = team.get('name', '')
                if team_name.lower() in question_lower:
                    detected_team = team_name
                    break
    
    # Determine entity type
    if detected_player:
        return {
            'entity_type': 'player',
            'entity_name': detected_player,
            'league_id': league_id
        }
    
    if detected_team:
        return {
            'entity_type': 'team', 
            'entity_name': detected_team,
            'league_id': league_id
        }
    
    # Check keywords
    player_score = sum(1 for kw in player_keywords if kw in question_lower)
    team_score = sum(1 for kw in team_keywords if kw in question_lower)
    league_score = sum(1 for kw in league_keywords if kw in question_lower)
    
    if league_score > player_score and league_score > team_score:
        return {
            'entity_type': 'league',
            'entity_name': None,
            'league_id': league_id
        }
    elif team_score > player_score:
        return {
            'entity_type': 'team',
            'entity_name': None,
            'league_id': league_id
        }
    elif player_score > 0:
        return {
            'entity_type': 'player',
            'entity_name': None,
            'league_id': league_id
        }
    
    return {
        'entity_type': 'general',
        'entity_name': None,
        'league_id': league_id
    }


def build_player_context(player_name: str, league_id: Optional[str] = None) -> Dict:
    """
    Build context for a specific player by fetching relevant data from Supabase
    """
    context = {
        'type': 'player',
        'player_name': player_name,
        'recent_games': [],
        'season_averages': {},
        'team_info': {}
    }
    
    try:
        # Fetch recent game stats
        query = supabase.table("player_stats").select("*").ilike("full_name", f"%{player_name}%")
        if league_id:
            query = query.eq("league_id", league_id)
        
        recent_games = query.order("numeric_id", desc=True).limit(5).execute()
        context['recent_games'] = recent_games.data if recent_games.data else []
        
        # Fetch season averages
        # Note: player_season_averages doesn't have league_id, so we rely on player name match
        # If we have recent_games, we can filter by team_id instead
        if context['recent_games'] and league_id:
            team_id = context['recent_games'][0].get('team_id')
            avg_query = supabase.table("player_season_averages").select("*").ilike("full_name", f"%{player_name}%").eq("team_id", team_id)
        else:
            avg_query = supabase.table("player_season_averages").select("*").ilike("full_name", f"%{player_name}%")
        
        averages = avg_query.execute()
        context['season_averages'] = averages.data[0] if averages.data else {}
        
        # Fetch player's team info
        if context['recent_games']:
            team_id = context['recent_games'][0].get('team_id')
            if team_id:
                team = supabase.table("teams").select("*").eq("team_id", team_id).execute()
                context['team_info'] = team.data[0] if team.data else {}
        
        logging.info(f"✅ Built player context for {player_name}: {len(context['recent_games'])} games")
        
    except Exception as e:
        logging.error(f"❌ Error building player context: {e}")
    
    return context


def build_team_context(team_name: str, league_id: Optional[str] = None) -> Dict:
    """
    Build context for a specific team by fetching relevant data from Supabase
    """
    context = {
        'type': 'team',
        'team_name': team_name,
        'team_info': {},
        'recent_games': [],
        'roster': [],
        'team_stats': []
    }
    
    try:
        # Fetch team info
        team_query = supabase.table("teams").select("*").ilike("name", f"%{team_name}%")
        if league_id:
            team_query = team_query.eq("league_id", league_id)
        
        team_result = team_query.execute()
        if team_result.data:
            context['team_info'] = team_result.data[0]
            team_id = team_result.data[0].get('team_id')
            
            # Fetch roster
            roster = supabase.table("players").select("*").eq("team_id", team_id).execute()
            context['roster'] = roster.data if roster.data else []
            
            # Fetch recent games
            games = supabase.table("game_schedule").select("*").or_(
                f"hometeam.ilike.%{team_name}%,awayteam.ilike.%{team_name}%"
            ).order("matchtime", desc=True).limit(5).execute()
            context['recent_games'] = games.data if games.data else []
            
            # Fetch team stats
            stats = supabase.table("team_stats").select("*").eq("team_id", team_id).order("numeric_id", desc=True).limit(5).execute()
            context['team_stats'] = stats.data if stats.data else []
        
        logging.info(f"✅ Built team context for {team_name}")
        
    except Exception as e:
        logging.error(f"❌ Error building team context: {e}")
    
    return context


def build_league_context(league_id: str) -> Dict:
    """
    Build context for league-level queries (top players, standings, etc.)
    """
    context = {
        'type': 'league',
        'league_id': league_id,
        'league_info': {},
        'teams': [],
        'top_scorers': [],
        'upcoming_games': []
    }
    
    try:
        # Fetch league info
        league = supabase.table("leagues").select("*").eq("league_id", league_id).execute()
        context['league_info'] = league.data[0] if league.data else {}
        
        # Fetch all teams
        teams = supabase.table("teams").select("*").eq("league_id", league_id).execute()
        context['teams'] = teams.data if teams.data else []
        
        # Fetch top scorers from season averages
        # Since player_season_averages doesn't have league_id, we filter by team_id
        if context['teams']:
            team_ids = [team['team_id'] for team in context['teams']]
            # Query with team_id filter using .in_() method
            top_scorers = supabase.table("player_season_averages").select("*").in_(
                "team_id", team_ids
            ).order("spoints", desc=True).limit(10).execute()
            context['top_scorers'] = top_scorers.data if top_scorers.data else []
        
        # Fetch upcoming games
        games = supabase.table("game_schedule").select("*").eq(
            "league_id", league_id
        ).order("matchtime", desc=False).limit(10).execute()
        context['upcoming_games'] = games.data if games.data else []
        
        logging.info(f"✅ Built league context for {league_id}")
        
    except Exception as e:
        logging.error(f"❌ Error building league context: {e}")
    
    return context


def build_general_context(league_id: Optional[str] = None) -> Dict:
    """
    Build general context for open-ended questions
    """
    context = {
        'type': 'general',
        'league_id': league_id
    }
    
    if league_id:
        # Add basic league context
        context.update(build_league_context(league_id))
    
    return context


def build_rag_context(question: str, league_id: Optional[str] = None, player_name: Optional[str] = None) -> Dict:
    """
    Main function to build RAG context based on the question
    
    Args:
        question: User's question
        league_id: Optional league context
        player_name: Optional player name hint from frontend
    
    Returns:
        Dict containing relevant context data
    """
    # Detect entities
    entities = detect_entities(question, league_id)
    
    # Override with player_name if provided
    if player_name:
        entities['entity_type'] = 'player'
        entities['entity_name'] = player_name
    
    logging.info(f"🔍 Detected entities: {entities}")
    
    # Build appropriate context
    if entities['entity_type'] == 'player' and entities['entity_name']:
        return build_player_context(entities['entity_name'], league_id)
    
    elif entities['entity_type'] == 'team' and entities['entity_name']:
        return build_team_context(entities['entity_name'], league_id)
    
    elif entities['entity_type'] == 'league' and league_id:
        return build_league_context(league_id)
    
    else:
        return build_general_context(league_id)
