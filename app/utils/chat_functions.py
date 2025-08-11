# app/utils/chat_functions.py

import os
import json
import openai
from app.utils.voiceflow_tools import get_player_stats

player_cache = {}

def store_player_data(thread_id, player_name, records):
    player_cache[thread_id] = {
        "player_name": player_name,
        "records": records
    }

def get_cached_player_data(thread_id):
    return player_cache.get(thread_id)


def create_assistant(client):
    assistant_file_path = os.path.join(os.path.dirname(__file__), '../../assistant.json')
    try:
        if os.path.exists(assistant_file_path):
            with open(assistant_file_path, 'r') as file:
                assistant_data = json.load(file)
                assistant_id = assistant_data['assistant_id']
                print("✅ Loaded existing assistant ID.")
        else:
            tools = [
                {
                    "type": "function",
                    "function": {
                        "name": "get_player_stats",
                        "description": "Retrieve one or more basketball stats for a player from Supabase. Use 'stat' for a single stat or 'stat_list' for multiple stats.",
                        "parameters": {
                            "type": "object",
                            "properties": {
                                "player_name": {
                                    "type": "string",
                                    "description": "Full name of the player (e.g. 'Rhys Farrell')"
                                },
                                "stat": {
                                    "type": "string",
                                    "description": "Optional. A single stat to retrieve (e.g. 'points', 'three_pt_percent')."
                                },
                                "stat_list": {
                                    "type": "array",
                                    "items": { "type": "string" },
                                    "description": "A list of stats to retrieve (e.g. ['points','assists','rebounds_total','team'])."
                                },
                                "mode": {
                                    "type": "string",
                                    "enum": ["latest", "total", "average"],
                                    "description": "Whether to return the latest, total, or average value."
                                },
                                "user_message": {
                                    "type": "string",
                                    "description": "The raw user query, used for inferring mode when not explicitly provided."
                                }
                            },
                            "required": ["player_name"]
                        }
                    }
                },
                {
                    "type": "function",
                    "function": {
                        "name": "summarize_player_stats",
                        "description": "Create a short scouting-style summary using the player's most recently fetched stat records.",
                        "parameters": {
                            "type": "object",
                            "properties": {
                                "thread_id": {
                                    "type": "string",
                                    "description": "The current thread ID for fetching cached stats."
                                }
                            },
                            "required": ["thread_id"]
                        }
                    }
                },
                {
                    "type": "function",
                    "function": {
                        "name": "get_top_players",
                        "description": "Get the top players in a specific stat category (e.g., 'Who leads in rebounds?', 'Top 5 scorers')",
                        "parameters": {
                            "type": "object",
                            "properties": {
                                "stat": {
                                    "type": "string",
                                    "description": "The stat to rank players by (e.g., 'points', 'rebounds', 'assists', 'steals')"
                                },
                                "limit": {
                                    "type": "integer",
                                    "description": "Number of top players to return (default 5, max 10)",
                                    "minimum": 1,
                                    "maximum": 10
                                },
                                "mode": {
                                    "type": "string",
                                    "enum": ["latest", "average", "total"],
                                    "description": "Whether to rank by latest game, per-game average, or season total"
                                },
                                "user_message": {
                                    "type": "string",
                                    "description": "The raw user query for inferring mode when not explicitly provided"
                                }
                            },
                            "required": ["stat"]
                        }
                    }
                },
                {
                    "type": "function",
                    "function": {
                        "name": "get_game_summary",
                        "description": "Get game information including scores, team comparisons, and key performances",
                        "parameters": {
                            "type": "object",
                            "properties": {
                                "game_date": {
                                    "type": "string",
                                    "description": "Game date in YYYY-MM-DD format (optional)"
                                },
                                "home_team": {
                                    "type": "string",
                                    "description": "Home team name (optional)"
                                },
                                "away_team": {
                                    "type": "string", 
                                    "description": "Away team name (optional)"
                                },
                                "query_type": {
                                    "type": "string",
                                    "enum": ["basic", "detailed", "team_comparison"],
                                    "description": "Type of summary: basic (score + top scorers), detailed (full breakdown), team_comparison (head-to-head stats)"
                                }
                            }
                        }
                    }
                },
                {
                    "type": "function", 
                    "function": {
                        "name": "get_team_analysis",
                        "description": "Analyze team performance, roster, bench production, or shooting",
                        "parameters": {
                            "type": "object",
                            "properties": {
                                "team_name": {
                                    "type": "string",
                                    "description": "Name of the team to analyze"
                                },
                                "analysis_type": {
                                    "type": "string",
                                    "enum": ["roster", "efficiency", "shooting_splits"],
                                    "description": "Type of analysis: roster (all players), efficiency (performance rankings), shooting_splits (team shooting percentages)"
                                },
                                "game_date": {
                                    "type": "string",
                                    "description": "Optional specific game date (YYYY-MM-DD)"
                                }
                            },
                            "required": ["team_name"]
                        }
                    }
                },
                {
                    "type": "function",
                    "function": {
                        "name": "get_advanced_insights", 
                        "description": "Generate advanced insights like top performers, starting 5 recommendations, or game impact analysis",
                        "parameters": {
                            "type": "object",
                            "properties": {
                                "insight_type": {
                                    "type": "string",
                                    "enum": ["top_performers", "starting_five", "game_impact"],
                                    "description": "Type of insight: top_performers (best overall), starting_five (recommend lineup), game_impact (highest +/- players)"
                                },
                                "limit": {
                                    "type": "integer",
                                    "description": "Number of results to return (default 5)",
                                    "minimum": 1,
                                    "maximum": 10
                                },
                                "team_filter": {
                                    "type": "string",
                                    "description": "Filter results by specific team (required for starting_five)"
                                },
                                "game_date": {
                                    "type": "string",
                                    "description": "Optional filter by specific game date (YYYY-MM-DD)"
                                }
                            },
                            "required": ["insight_type"]
                        }
                    }
                }
            ]


            # Create assistant
            assistant = client.beta.assistants.create(
                name="Basketball Stats Assistant",
                instructions="""
                You are Swish Assistant, a comprehensive basketball analyst with access to multiple tools for player stats, game analysis, and team insights.

                📌 CORE RULES:
                - Never guess or generate stat answers yourself
                - Always use the appropriate tool for each type of question
                - Choose the most specific tool for the user's query
                - Remember player names mentioned in the conversation for follow-up questions

                🧠 CONVERSATIONAL CONTEXT:
                - When a user asks follow-up questions like "who does he play for?", "what team?", "his rebounds?", etc., use the most recently mentioned player name
                - If no player name is provided and the question refers to "he/his/him", pass an empty string for player_name - the tool will use the last mentioned player
                - Keep track of conversation flow to provide contextual responses

                🏀 TOOL SELECTION GUIDE:

                **get_player_stats** - Individual player performance
                - "How many points did Rhys Farrell score?"
                - "What was Corey Samuels' shooting percentage?"
                - "How did James Claar perform last game?"
                - "Who does he play for?" → Use player_name = "" to reference last mentioned player
                - "His rebounds?" → Use player_name = "" to reference last mentioned player

                **get_top_players** - Rankings and leaderboards  
                - "Who leads in rebounds?"
                - "Top 5 scorers"
                - "Best assist leaders"

                **get_game_summary** - Game-level information
                - "What was the final score?"
                - "Which team won the rebound battle?" → query_type = "team_comparison"
                - "How did the teams compare?" → query_type = "team_comparison"
                - "Game summary" → query_type = "detailed"

                **get_team_analysis** - Team-specific analysis
                - "How did Hurricanes' roster perform?" → analysis_type = "roster"
                - "Show me shooting splits for Hurricanes" → analysis_type = "shooting_splits"  
                - "Who had the highest efficiency rating?" → analysis_type = "efficiency"

                **get_advanced_insights** - Advanced analytics
                - "Pick the top 5 performers" → insight_type = "top_performers"
                - "Choose a starting 5 for Hurricanes" → insight_type = "starting_five", team_filter = "Hurricanes"
                - "Who contributed most to the win?" → insight_type = "game_impact"
                - "Best overall performance" → insight_type = "top_performers"

                📊 Mode Detection (for player stats):
                - "average"/"per game" → mode = "average"
                - "total"/"season"/"overall" → mode = "total"  
                - "latest"/"last game"/no context → mode = "latest"

                🔄 Follow-up Examples:
                User: "How did Maximillian Matthews perform?"
                Assistant: [calls get_player_stats with player_name="Maximillian Matthews"]
                User: "Who does he play for?"
                Assistant: [calls get_player_stats with player_name="" and stat_list=["team"] to get team info]
                User: "His shooting percentage?"
                Assistant: [calls get_player_stats with player_name="" and stat="field_goal_percent"]

                Always use tools - never provide basketball analysis from your own knowledge.
                """,

                model="gpt-4-1106-preview",
                tools=tools
            )

            with open(assistant_file_path, 'w') as file:
                json.dump({'assistant_id': assistant.id}, file)
                print("✅ Created new assistant and saved ID.")

            assistant_id = assistant.id

        return assistant_id

    except Exception as e:
        print(f"❌ Error in create_assistant: {e}")
        raise
