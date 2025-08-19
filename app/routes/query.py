from flask import Blueprint, request, jsonify, Response
from flask_limiter import Limiter
from flask_limiter.util import get_remote_address
from openai import OpenAI
from typing import cast
import openai
import os
import time
import json
import asyncio
import logging
from app.utils.chat_data import supabase
from flask_cors import CORS
from threading import Thread
from app.utils.chat_functions import create_assistant, store_player_data
from app.utils.voiceflow_tools import get_player_stats, get_top_players, get_game_summary, get_team_analysis, get_advanced_insights, get_player_trending
from openai.types.chat import ChatCompletionMessageParam

# Blueprint setup
query_bp = Blueprint("query", __name__)
CORS(query_bp, resources={r"/*": {"origins": "*"}})

# OpenAI client and assistant creation
client = OpenAI(api_key=os.environ['OPENAI_API_KEY'])
assistant_id = create_assistant(client)

# Rate limiter
limiter = Limiter(
    key_func=get_remote_address,
    default_limits=["100 per day", "10 per minute"],
    storage_uri="memory://"
)

# In-memory cache for delayed responses
ai_summaries = {}

@query_bp.route('/start', methods=['GET'])
@limiter.limit("30 per minute")
def start_conversation():
    thread = client.beta.threads.create()
    return jsonify({"thread_id": thread.id})

@query_bp.route('/reset', methods=['GET'])
@limiter.limit("10 per minute")
def reset_thread():
    try:
        thread = client.beta.threads.create()
        return jsonify({
            "thread_id": thread.id,
            "message": "🧼 New conversation started."
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@query_bp.route('/chat', methods=['POST'])
@limiter.limit("60 per minute")
def chat():
    try:
        data = request.json
        if not data:
            return jsonify({"error": "No JSON data provided"}), 400

        thread_id = data.get('thread_id')
        user_input = data.get('message', '')
        chat_mode = data.get('chatMode', 'general')
        player_name = data.get('player_name')

        logging.warning(f"📦 Incoming POST: {data}")
        logging.warning(f"🎯 Extracted player name: {player_name}")

        # Submit user message to thread
        client.beta.threads.messages.create(
            thread_id=thread_id,
            role="user",
            content=user_input
        )

        # Let OpenAI Assistant handle tool use
        run = client.beta.threads.runs.create_and_poll(
            thread_id=thread_id,
            assistant_id=assistant_id
        )

        # If Assistant wants to call a tool
        if run.status == "requires_action" and run.required_action:
            tool_call = run.required_action.submit_tool_outputs.tool_calls[0]
            tool_name = tool_call.function.name
            args = json.loads(tool_call.function.arguments)

            if tool_name == "get_player_stats":
                response = asyncio.run(get_player_stats(**args))

                if isinstance(response, tuple):
                    raw_output, records = response
                    store_player_data(thread_id, args.get("player_name"), records)
                else:
                    raw_output = response
                    
            elif tool_name == "get_top_players":
                raw_output = asyncio.run(get_top_players(**args))
                
            elif tool_name == "get_game_summary":
                raw_output = asyncio.run(get_game_summary(**args))
                
            elif tool_name == "get_team_analysis":
                raw_output = asyncio.run(get_team_analysis(**args))
                
            elif tool_name == "get_player_trending":
                raw_output = asyncio.run(get_player_trending(**args))
                
            elif tool_name == "get_advanced_insights":
                raw_output = asyncio.run(get_advanced_insights(**args))

                # Start GPT summary thread (optional)
                def run_gpt_summary():
                    try:
                        logging.warning("🧠 Starting GPT summary thread...")
                        logging.warning(f"🧾 Prompt to GPT:\n{raw_output}")

                        ai_summary = client.chat.completions.create(
                            model="gpt-3.5-turbo-1106",
                            temperature=0.9,
                            messages=[
                                {
                                    "role": "system",
                                    "content": """
        You are a basketball scout. Given the player's stat summary, return a structured scouting report with:
        1. Player Name + Headline
        2. Key Stats (bullets)
        3. Strengths
        4. Weaknesses
        5. Summary (1 short paragraph)
        6. How to Defend (with disclaimer: 'Use this as a guide. You know your team best.')
        """
                                },
                                {"role": "user", "content": raw_output}
                            ]
                        )

                        result = ai_summary.choices[0].message.content
                        ai_summaries[thread_id] = result
                        logging.warning("✅ GPT summary generated successfully.")
                        logging.warning(result)

                    except Exception as e:
                        logging.error("❌ GPT summary failed:", exc_info=True)
                        ai_summaries[thread_id] = f"⚠️ Error generating full summary: {str(e)}"

                Thread(target=run_gpt_summary).start()

                return jsonify({
                    "response": raw_output,
                    "thread_id": thread_id,
                    "gpt_status": "processing"
                })
            else:
                # For other tools, return the response directly
                return jsonify({
                    "response": raw_output,
                    "thread_id": thread_id
                })

        # If nothing useful happened
        return jsonify({"error": "❌ No tool call triggered."}), 400

    except Exception as e:
        logging.error("❌ Error in /chat route:", exc_info=True)
        return jsonify({"error": str(e)}), 500


@query_bp.route('/check_summary', methods=['POST'])
@limiter.limit("60 per minute")
def check_summary():
    try:
        data = request.json
        thread_id = data.get('thread_id')

        if not thread_id:
            return jsonify({"error": "Missing thread_id"}), 400

        summary = ai_summaries.get(thread_id)

        if summary:
            return jsonify({"summary": summary, "ready": True})
        else:
            return jsonify({"ready": False})

    except Exception as e:
        logging.error("❌ Error in /check_summary route:", exc_info=True)
        return jsonify({"error": str(e)}), 500

@query_bp.route('/api/chat/league', methods=['POST'])
@limiter.limit("60 per minute")
def chat_league():
    """Handle league-specific chat requests"""
    try:
        data = request.json
        if not data:
            return jsonify({"error": "No JSON data provided"}), 400

        # Extract parameters - handle both 'message' and 'question' for compatibility
        thread_id = data.get('thread_id')
        user_input = data.get('message', '') or data.get('question', '')
        league_id = data.get('league_id')
        player_name = data.get('player_name')
        
        # If no thread_id provided, create a new thread
        if not thread_id:
            thread = client.beta.threads.create()
            thread_id = thread.id
            logging.warning(f"🆕 Created new thread: {thread_id}")

        if not league_id:
            return jsonify({"error": "league_id is required"}), 400

        logging.warning(f"📦 League chat request: {data}")
        logging.warning(f"🎯 User input: '{user_input}'")
        logging.warning(f"🏀 League ID: '{league_id}'")
        logging.warning(f"👤 Player name: '{player_name}'")

        # Submit user message to thread
        client.beta.threads.messages.create(
            thread_id=thread_id,
            role="user",
            content=user_input
        )

        # Run assistant
        run = client.beta.threads.runs.create_and_poll(
            thread_id=thread_id,
            assistant_id=assistant_id
        )

        # Handle tool calls (same logic as /chat route)
        if run.status == "requires_action" and run.required_action:
            tool_call = run.required_action.submit_tool_outputs.tool_calls[0]
            tool_name = tool_call.function.name
            args = json.loads(tool_call.function.arguments)

            # Ensure league_id is always passed to tools that support it
            args['league_id'] = league_id
            
            logging.warning(f"🔧 Tool call: {tool_name} with args: {args}")

            try:
                if tool_name == "get_player_stats":
                    response = asyncio.run(get_player_stats(**args))
                    if isinstance(response, tuple):
                        raw_output, records = response
                        store_player_data(thread_id, args.get("player_name"), records)
                    else:
                        raw_output = response
                        
                elif tool_name == "get_top_players":
                    raw_output = asyncio.run(get_top_players(**args))
                    
                elif tool_name == "get_game_summary":
                    raw_output = asyncio.run(get_game_summary(**args))
                    
                elif tool_name == "get_team_analysis":
                    raw_output = asyncio.run(get_team_analysis(**args))
                    
                elif tool_name == "get_player_trending":
                    raw_output = asyncio.run(get_player_trending(**args))
                    
                elif tool_name == "get_advanced_insights":
                    raw_output = asyncio.run(get_advanced_insights(**args))
                else:
                    raw_output = "Tool not recognized"
                    
                logging.warning(f"✅ Tool {tool_name} returned: {raw_output[:200]}...")
                
            except Exception as tool_error:
                logging.error(f"❌ Tool {tool_name} failed: {tool_error}")
                raw_output = f"Error executing {tool_name}: {str(tool_error)}"

            # Submit tool output and wait for completion
            run = client.beta.threads.runs.submit_tool_outputs_and_poll(
                thread_id=thread_id,
                run_id=run.id,
                tool_outputs=[{
                    "tool_call_id": tool_call.id,
                    "output": str(raw_output)
                }]
            )

            # Get the assistant's final response
            messages = client.beta.threads.messages.list(thread_id=thread_id)
            assistant_response = messages.data[0].content[0].text.value if messages.data else str(raw_output)
            
            return jsonify({
                "response": assistant_response,
                "thread_id": thread_id,
                "league_id": league_id
            })

        return jsonify({"error": "❌ No tool call triggered."}), 400

    except Exception as e:
        logging.error("❌ Error in /api/chat/league route:", exc_info=True)
        return jsonify({"error": str(e)}), 500

@query_bp.route('/api/generate-summary', methods=['POST'])
def generate_summary():
    data = request.get_json()

    if not data:
        return jsonify({"summary": "⚠️ Missing JSON payload."}), 400

    player_name = data.get("name")
    team = data.get("team")
    game_date = data.get("game_date")

    if not player_name or not team or not game_date:
        return jsonify({"summary": "⚠️ Missing required fields."}), 400

    try:
        # Query Supabase using correct field names
        response = supabase.table("player_stats").select("*").eq("name", player_name).eq("team", team).eq("game_date", game_date).limit(1).execute()
        player_data = response.data[0] if response.data else None

        if not player_data:
            return jsonify({"summary": "⚠️ Player game data not found."}), 404

        fields = [
            "points", "rebounds_total", "assists", "steals", "blocks", "turnovers",
            "fgm", "fga", "three_pm", "three_pa", "ftm", "fta",
            "offensive_rebounds", "defensive_rebounds", "minutes",
            "plus_minus", "personal_fouls", "field_goal_pct",
            "three_pt_pct", "free_throw_pct"
        ]

        statline = "\n".join([
            f"{field.replace('_', ' ').title()}: {player_data.get(field, 'N/A')}"
            for field in fields
        ])

        prompt = f"""
Player: {player_name}
Team: {team}
Game Date: {game_date}

Statline:
{statline}

You are a basketball coach writing a post-game report. Provide:
1. A title headline.
2. One short paragraph summary of the player's performance.
3. A coaching tip or takeaway based on the data.
"""

        result = openai.chat.completions.create(
            model="gpt-4",
            messages=[
                {"role": "system", "content": "You are a basketball coach generating post-game summaries."},
                {"role": "user", "content": prompt},
            ],
            temperature=0.7,
        )

        return jsonify({"summary": result.choices[0].message.content})

    except Exception as e:
        return jsonify({"summary": f"⚠️ Error generating summary: {str(e)}"}), 500