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
from app.utils.rag_utils import build_rag_context
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
    """
    RAG-based chat endpoint
    Fetches relevant context from Supabase and sends it to OpenAI Agent
    """
    try:
        data = request.json
        if not data:
            return jsonify({"error": "No JSON data provided"}), 400

        thread_id = data.get('thread_id')
        user_input = data.get('message', '')
        league_id = data.get('league_id')
        player_name = data.get('player_name')

        logging.info(f"📦 RAG Chat Request: question='{user_input}', league_id={league_id}, player_name={player_name}")

        # Create thread if not provided
        if not thread_id:
            thread = client.beta.threads.create()
            thread_id = thread.id
            logging.info(f"🆕 Created new thread: {thread_id}")

        # Build RAG context from Supabase
        context = build_rag_context(user_input, league_id=league_id, player_name=player_name)
        
        logging.info(f"📊 Built context type: {context.get('type')}")

        # Format context as a structured message
        context_message = f"""
CONTEXT DATA FROM DATABASE:
{json.dumps(context, indent=2, default=str)}

USER QUESTION: {user_input}

Please answer the question using ONLY the data provided in the CONTEXT above. 
Be specific with numbers and stats. If the data doesn't contain the answer, say so.
"""

        # Submit context + question to thread
        client.beta.threads.messages.create(
            thread_id=thread_id,
            role="user",
            content=context_message
        )

        # Run the assistant (no tool calling needed)
        run = client.beta.threads.runs.create_and_poll(
            thread_id=thread_id,
            assistant_id=assistant_id
        )

        # Get the assistant's response
        if run.status == "completed":
            messages = client.beta.threads.messages.list(thread_id=thread_id, limit=1)
            if messages.data and messages.data[0].content:
                content_block = messages.data[0].content[0]
                # Safely extract text content
                assistant_message = content_block.text.value if hasattr(content_block, 'text') else str(content_block)
                
                logging.info(f"✅ RAG response generated successfully")
                
                return jsonify({
                    "response": assistant_message,
                    "thread_id": thread_id,
                    "context_type": context.get('type')
                })
        
        # Handle other statuses
        return jsonify({
            "error": f"Assistant run failed with status: {run.status}",
            "thread_id": thread_id
        }), 500

    except Exception as e:
        logging.error("❌ Error in /chat route:", exc_info=True)
        return jsonify({"error": str(e)}), 500


@query_bp.route('/check_summary', methods=['POST'])
@limiter.limit("60 per minute")
def check_summary():
    try:
        data = request.json
        if not data:
            return jsonify({"error": "No JSON data provided"}), 400
        
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
    """
    League-specific RAG chat endpoint
    Redirects to main chat endpoint with league context
    """
    try:
        data = request.json
        if not data:
            return jsonify({"error": "No JSON data provided"}), 400

        # Extract and normalize parameters
        thread_id = data.get('thread_id')
        user_input = data.get('message', '') or data.get('question', '')
        league_id = data.get('league_id')
        player_name = data.get('player_name')
        
        if not league_id:
            return jsonify({"error": "league_id is required"}), 400

        logging.info(f"📦 League RAG Chat: question='{user_input}', league_id={league_id}")

        # Create thread if not provided
        if not thread_id:
            thread = client.beta.threads.create()
            thread_id = thread.id
            logging.info(f"🆕 Created new thread: {thread_id}")

        # Build RAG context with league filter
        context = build_rag_context(user_input, league_id=league_id, player_name=player_name)
        
        logging.info(f"📊 Built context type: {context.get('type')}")

        # Format context message
        context_message = f"""
CONTEXT DATA FROM DATABASE (League: {league_id}):
{json.dumps(context, indent=2, default=str)}

USER QUESTION: {user_input}

Please answer the question using ONLY the data provided in the CONTEXT above. 
Be specific with numbers and stats. If the data doesn't contain the answer, say so.
"""

        # Submit to thread
        client.beta.threads.messages.create(
            thread_id=thread_id,
            role="user",
            content=context_message
        )

        # Run assistant
        run = client.beta.threads.runs.create_and_poll(
            thread_id=thread_id,
            assistant_id=assistant_id
        )

        # Get response
        if run.status == "completed":
            messages = client.beta.threads.messages.list(thread_id=thread_id, limit=1)
            if messages.data and messages.data[0].content:
                content_block = messages.data[0].content[0]
                # Safely extract text content
                assistant_message = content_block.text.value if hasattr(content_block, 'text') else str(content_block)
                
                logging.info(f"✅ League RAG response generated")
                
                return jsonify({
                    "response": assistant_message,
                    "thread_id": thread_id,
                    "league_id": league_id,
                    "context_type": context.get('type')
                })
        
        return jsonify({
            "error": f"Assistant run failed with status: {run.status}",
            "thread_id": thread_id
        }), 500

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