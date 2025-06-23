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
                                    "description": "A list of stats to retrieve (e.g. ['points','assists','rebounds_total'])."
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
                }
            ]


            # Create assistant
            assistant = client.beta.assistants.create(
                name="Basketball Stats Assistant",
                instructions="""
                You are Swish Assistant, a basketball stat analyst that must use the `get_player_stats` tool to respond to any player performance questions.

                📌 RULES:
                - Never guess or generate stat answers yourself.
                - Always call the `get_player_stats` function if a player is mentioned, even if the user doesn't specify a stat.
                - If no stat or stat_list is provided, the tool will return all available stats automatically.

                📊 Mode Detection:
                - If user asks about "average" or "per game" → use mode = "average"
                - If user says "total", "combined", or "overall" → use mode = "total"
                - If they mention a recent game or give no time context → use mode = "latest"

                🧠 Examples:
                - "How many assists does he average?" → stat = "assists", mode = "average"
                - "How many points has Rhys Farrell scored?" → stat = "points", mode = "total"
                - "How did Rhys Farrell do last game?" → omit stat/stat_list, mode = "latest"
                - "What were his stats in the last match?" → omit stat/stat_list, mode = "latest"
                - "Show me all his stats" → omit stat/stat_list

                Never respond to performance or stat-related questions using your own words. Always call the tool.
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
