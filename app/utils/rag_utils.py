"""
RAG Utilities for Swish Assistant
Provides entity detection and context building for basketball analytics queries.
Queries SQL views for clean, structured data.
"""

import logging
from typing import Dict, Optional
from app.utils.chat_data import supabase

log = logging.getLogger("rag_utils")


def detect_entities(question: str, league_id: Optional[str] = None) -> Dict:
    """
    Detect entities (player, team, league) from the user question.
    Returns a dict with entity_type, entity_name, and league_id.
    """
    question_lower = question.lower()

    player_keywords = ['player', 'scored', 'points', 'rebounds', 'assists', 'stats', 'performance', 'game', 'average', 'season']
    team_keywords   = ['team', 'roster', 'record', 'vs', 'against', 'played', 'win', 'loss']
    league_keywords = ['league', 'top', 'leaders', 'best', 'ranking', 'standings', 'leading']

    detected_player = None
    detected_team   = None

    if league_id:
        try:
            players_result = supabase.table("players").select("full_name").eq("league_id", league_id).execute()
            if players_result.data:
                for player in players_result.data:
                    name = player.get('full_name', '')
                    if name and name.lower() in question_lower:
                        detected_player = name
                        break
        except Exception as e:
            log.warning("Player entity detection failed: %s", e)

        if not detected_player:
            try:
                # Also search player_stats for names not yet in players registry
                words = [w for w in question_lower.split() if len(w) > 3]
                for word in words:
                    result = supabase.table("player_stats").select("full_name").ilike("full_name", f"%{word}%").eq("league_id", league_id).limit(1).execute()
                    if result.data:
                        detected_player = result.data[0].get("full_name")
                        if detected_player and detected_player.lower() in question_lower:
                            break
                        detected_player = None
            except Exception as e:
                log.warning("Fallback player detection failed: %s", e)

        try:
            teams_result = supabase.table("teams").select("name").eq("league_id", league_id).execute()
            if teams_result.data:
                for team in teams_result.data:
                    name = team.get('name', '')
                    if name and name.lower() in question_lower:
                        detected_team = name
                        break
        except Exception as e:
            log.warning("Team entity detection failed: %s", e)

    if detected_player:
        return {'entity_type': 'player', 'entity_name': detected_player, 'league_id': league_id}

    if detected_team:
        return {'entity_type': 'team', 'entity_name': detected_team, 'league_id': league_id}

    player_score = sum(1 for kw in player_keywords if kw in question_lower)
    team_score   = sum(1 for kw in team_keywords   if kw in question_lower)
    league_score = sum(1 for kw in league_keywords if kw in question_lower)

    if league_score > player_score and league_score > team_score:
        return {'entity_type': 'league', 'entity_name': None, 'league_id': league_id}
    elif team_score > player_score:
        return {'entity_type': 'team',   'entity_name': None, 'league_id': league_id}
    elif player_score > 0:
        return {'entity_type': 'player', 'entity_name': None, 'league_id': league_id}

    return {'entity_type': 'general', 'entity_name': None, 'league_id': league_id}


def build_player_context(player_name: str, league_id: Optional[str] = None) -> Dict:
    """
    Build context for a specific player from the views.
    """
    context = {
        'type': 'player',
        'player_name': player_name,
        'recent_games': [],
        'season_averages': {},
        'advanced_stats': [],
        'team_info': {}
    }

    # Recent traditional game log
    try:
        query = supabase.table("v_player_game_log").select("*").ilike("player_name", f"%{player_name}%")
        if league_id:
            query = query.eq("league_id", league_id)
        result = query.limit(5).execute()
        context['recent_games'] = result.data if result.data else []
        log.info("Player '%s': %d recent games", player_name, len(context['recent_games']))
    except Exception as e:
        log.error("Error fetching player game log: %s", e)

    # Season averages
    try:
        query = supabase.table("v_player_season_averages").select("*").ilike("player_name", f"%{player_name}%")
        if league_id:
            query = query.eq("league_id", league_id)
        result = query.execute()
        context['season_averages'] = result.data[0] if result.data else {}
    except Exception as e:
        log.error("Error fetching player season averages: %s", e)

    # Advanced stats (last 3 games)
    try:
        query = supabase.table("v_player_advanced_game").select("*").ilike("player_name", f"%{player_name}%")
        if league_id:
            query = query.eq("league_id", league_id)
        result = query.limit(3).execute()
        context['advanced_stats'] = result.data if result.data else []
    except Exception as e:
        log.error("Error fetching player advanced stats: %s", e)

    # Team info
    try:
        if context['recent_games']:
            team_id = context['recent_games'][0].get('team_id')
            if team_id:
                team = supabase.table("teams").select("*").eq("team_id", team_id).execute()
                context['team_info'] = team.data[0] if team.data else {}
    except Exception as e:
        log.error("Error fetching team info: %s", e)

    return context


def build_team_context(team_name: str, league_id: Optional[str] = None) -> Dict:
    """
    Build context for a specific team from the views.
    """
    context = {
        'type': 'team',
        'team_name': team_name,
        'team_info': {},
        'recent_games': [],
        'season_averages': {},
        'advanced_stats': [],
        'roster': []
    }

    team_id = None

    # Team info + ID
    try:
        query = supabase.table("teams").select("*").ilike("name", f"%{team_name}%")
        if league_id:
            query = query.eq("league_id", league_id)
        result = query.execute()
        if result.data:
            context['team_info'] = result.data[0]
            team_id = result.data[0].get('team_id')
    except Exception as e:
        log.error("Error fetching team info: %s", e)

    # Recent game log
    try:
        query = supabase.table("v_team_game_log").select("*").ilike("team_name", f"%{team_name}%")
        if league_id:
            query = query.eq("league_id", league_id)
        result = query.limit(5).execute()
        context['recent_games'] = result.data if result.data else []
    except Exception as e:
        log.error("Error fetching team game log: %s", e)

    # Season averages
    try:
        query = supabase.table("v_team_season_averages").select("*").ilike("team_name", f"%{team_name}%")
        if league_id:
            query = query.eq("league_id", league_id)
        result = query.execute()
        context['season_averages'] = result.data[0] if result.data else {}
    except Exception as e:
        log.error("Error fetching team season averages: %s", e)

    # Advanced stats
    try:
        query = supabase.table("v_team_advanced_game").select("*").ilike("team_name", f"%{team_name}%")
        if league_id:
            query = query.eq("league_id", league_id)
        result = query.limit(3).execute()
        context['advanced_stats'] = result.data if result.data else []
    except Exception as e:
        log.error("Error fetching team advanced stats: %s", e)

    # Roster
    try:
        if team_id:
            roster = supabase.table("players").select("full_name, shirtNumber, playingposition").eq("team_id", team_id).execute()
            context['roster'] = roster.data if roster.data else []
    except Exception as e:
        log.error("Error fetching roster: %s", e)

    log.info("Built team context for '%s'", team_name)
    return context


def build_league_context(league_id: str) -> Dict:
    """
    Build context for league-level queries using the views.
    """
    context = {
        'type': 'league',
        'league_id': league_id,
        'league_info': {},
        'teams': [],
        'top_scorers': [],
        'top_assisters': [],
        'top_rebounders': [],
        'upcoming_games': [],
        'recent_games': []
    }

    # League info
    try:
        result = supabase.table("leagues").select("*").eq("league_id", league_id).execute()
        context['league_info'] = result.data[0] if result.data else {}
    except Exception as e:
        log.error("Error fetching league info: %s", e)

    # Teams
    try:
        result = supabase.table("teams").select("*").eq("league_id", league_id).execute()
        context['teams'] = result.data if result.data else []
    except Exception as e:
        log.error("Error fetching teams: %s", e)

    # League leaders from view — top 10 per category
    try:
        result = supabase.table("v_league_leaders").select(
            "player_name,team_name,games_played,avg_pts,avg_ast,avg_reb,avg_stl,avg_blk,pts_rank,ast_rank,reb_rank"
        ).eq("league_id", league_id).lte("pts_rank", 10).order("pts_rank").execute()
        context['top_scorers'] = result.data if result.data else []
    except Exception as e:
        log.error("Error fetching league leaders: %s", e)

    # Upcoming games
    try:
        result = supabase.table("v_upcoming_games").select("*").eq("league_id", league_id).limit(10).execute()
        context['upcoming_games'] = result.data if result.data else []
    except Exception as e:
        log.error("Error fetching upcoming games: %s", e)

    # Recent results
    try:
        result = supabase.table("v_recent_games").select("*").eq("league_id", league_id).limit(5).execute()
        context['recent_games'] = result.data if result.data else []
    except Exception as e:
        log.error("Error fetching recent games: %s", e)

    log.info("Built league context for league_id=%s", league_id)
    return context


def build_general_context(league_id: Optional[str] = None) -> Dict:
    """
    Build general context for open-ended questions.
    """
    context = {'type': 'general', 'league_id': league_id}
    if league_id:
        context.update(build_league_context(league_id))
    return context


def build_rag_context(question: str, league_id: Optional[str] = None, player_name: Optional[str] = None) -> Dict:
    """
    Main function to build RAG context based on the question.
    Detects entities and routes to the appropriate context builder.
    """
    entities = detect_entities(question, league_id)

    # Frontend-supplied player name takes priority
    if player_name:
        entities['entity_type'] = 'player'
        entities['entity_name'] = player_name

    log.info("Detected entities: %s", entities)

    if entities['entity_type'] == 'player' and entities['entity_name']:
        return build_player_context(entities['entity_name'], league_id)

    if entities['entity_type'] == 'team' and entities['entity_name']:
        return build_team_context(entities['entity_name'], league_id)

    if entities['entity_type'] == 'league' and league_id:
        return build_league_context(league_id)

    return build_general_context(league_id)
