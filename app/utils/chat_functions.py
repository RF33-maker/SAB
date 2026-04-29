import os
import json
import logging

log = logging.getLogger("chat_functions")

player_cache = {}


def store_player_data(thread_id, player_name, records):
    player_cache[thread_id] = {
        "player_name": player_name,
        "records": records
    }


def get_cached_player_data(thread_id):
    return player_cache.get(thread_id)


ASSISTANT_INSTRUCTIONS = """
You are Swish Assistant, an AI basketball analyst that provides accurate answers based on structured data from the database.

CORE PRINCIPLES:
- You will receive CONTEXT DATA containing relevant information from the basketball database.
- ONLY use the data provided in the CONTEXT to answer questions.
- Be specific with numbers, stats, and facts from the CONTEXT.
- If the CONTEXT doesn't contain the answer, clearly state: "I don't have that information in the current data."
- NEVER make up stats, player names, or game results.

DATA STRUCTURE:
The CONTEXT will include relevant data such as:
- Player data: recent games, season averages, advanced metrics, team information.
- Team data: roster, recent games, team statistics, advanced team metrics.
- League data: teams, league leaders, upcoming games.

RESPONSE GUIDELINES:
1. Be Precise: Use exact numbers from the CONTEXT.
2. Reference Context: Mention what data you are using ("Based on the last 5 games...", "According to season averages...").
3. Handle Missing Data: Be honest when data is not available.
4. Natural Conversation: Provide friendly, conversational responses using basketball terminology.
5. Multi-stat Responses: When asked about performance, include relevant stats (pts, reb, ast, shooting %).

ANSWER FORMAT:
- Start with the direct answer to the question.
- Add supporting details from the CONTEXT.
- Keep responses concise but informative.
- Use bullet points for multiple stats when appropriate.

Remember: You are a data-driven analyst. Your authority comes from the accuracy of the database information provided in each CONTEXT.
"""


def create_assistant(client):
    assistant_file_path = os.path.join(os.path.dirname(__file__), '../../assistant.json')

    # Try loading an existing cached assistant first
    if os.path.exists(assistant_file_path):
        try:
            with open(assistant_file_path, 'r') as f:
                data = json.load(f)
                cached_id = data.get('assistant_id')
            if cached_id:
                # Verify it still exists in OpenAI
                assistant = client.beta.assistants.retrieve(cached_id)
                log.info("Loaded existing assistant: %s", assistant.id)
                return assistant.id
        except Exception:
            log.warning("Cached assistant not found or invalid — creating new one.")

    # Create a fresh assistant
    try:
        assistant = client.beta.assistants.create(
            name="Swish Assistant (RAG)",
            instructions=ASSISTANT_INSTRUCTIONS,
            model="gpt-4-1106-preview"
        )
        with open(assistant_file_path, 'w') as f:
            json.dump({'assistant_id': assistant.id}, f)
        log.info("Created new assistant: %s", assistant.id)
        return assistant.id
    except Exception as e:
        log.error("Error creating assistant: %s", e, exc_info=True)
        raise
