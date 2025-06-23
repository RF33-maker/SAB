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
from app.utils.voiceflow_tools import get_player_stats
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

        # If nothing useful happened
        return jsonify({"error": "❌ No tool call triggered."}), 400


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
