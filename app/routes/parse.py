from flask import Blueprint, request, jsonify
from app.utils.chat_data import supabase
from app.utils.parser import extract_player_stats
import traceback
from datetime import datetime
import io
import json

parse_bp = Blueprint("parse", __name__)

def log(msg: str):
    print(msg, flush=True)

def log_to_file(msg):
    full_msg = f"[{datetime.utcnow().isoformat()}] {msg}"
    with open("parse_log.txt", "a") as f:
        f.write(full_msg + "\n")
    print(full_msg, flush=True)

log_to_file("🔧 Logging is working.")

@parse_bp.route("/api/parse", methods=["POST"])
def handle_parse():
    try:
        data = request.get_json()
        if not data:
            log_to_file("❌ Request did not include valid JSON.")
            return jsonify({"error": "Missing or invalid JSON in request body"}), 400

        file_path = data.get("file_path")
        user_id = data.get("user_id")
        league_id = data.get("league_id")

        if not file_path or not user_id or not league_id:
            log_to_file("[❌] Missing file_path or user_id or league_id")
            return jsonify({"error": "file_path, user_id and league_id are required"}), 400

        log_to_file(f"[📥] Parsing file for user: {user_id}")
        log_to_file(f"[📄] File path: {file_path}")

        try:
            response = supabase.storage.from_("user-uploads").download(file_path)
        except Exception as e:
            log_to_file(f"❌ Supabase threw an exception: {e}")
            return jsonify({"error": f"Supabase threw: {str(e)}"}), 500

        if not response:
            log_to_file("❌ Supabase returned empty response — file likely missing or blocked")
            return jsonify({"error": "Supabase returned empty response"}), 404

        log_to_file("✅ Supabase file response received")

        try:
            pdf_file = io.BytesIO(response)
        except Exception as e:
            log_to_file(f"❌ Writing to local file failed: {e}")
            return jsonify({"error": f"Write failed: {str(e)}"}), 500

        # 👇 Parse the PDF and return both players and game info
        try:
            result = extract_player_stats(pdf_file, league_id)
        except Exception as e:
            log_to_file(f"🚨 extract_player_stats error: {e}")
            return jsonify({"error": f"Parse failed: {str(e)}"}), 500

        players = result.get("players", [])
        game = result.get("game", {})

        log_to_file(f"✅ Parsed player count: {len(players)}")
        if not players:
            log_to_file("❌ No players returned from parser.")
            return jsonify({"error": "No player data extracted from PDF"}), 400

        # 📦 Add metadata to player rows
        for player in players:
            player["user_id"] = user_id
            player["league_id"] = league_id
            player["is_public"] = True

        # 📦 Add metadata to game row
        game["league_id"] = league_id
        game["pdf_url"] = file_path

        # ✅ Insert game first
        try:
            game_response = supabase.table("games").insert(game).execute()
            log_to_file(f"📦 Game insert response: {game_response}")
        except Exception as e:
            log_to_file(f"❌ Game insert failed: {e}")
            return jsonify({"error": f"Game insert failed: {str(e)}"}), 500

        # ✅ Then insert players
        try:
            insert_response = supabase.table("player_stats").insert(players).execute()
            log_to_file(f"📥 Player insert response: {insert_response}")
        except Exception as e:
            log_to_file(f"❌ Player insert failed: {e}")
            return jsonify({"error": f"Player insert failed: {str(e)}"}), 500

        log_to_file(f"🎯 Successfully inserted {len(players)} players and 1 game.")
        return jsonify({
            "status": "success",
            "records_added": len(players),
            "example": players[0],
            "game_id": game.get("id")
        })

    except Exception as e:
        err_msg = f"🔥 Fatal error in /api/parse: {str(e)}"
        log_to_file(err_msg)
        traceback.print_exc()
        return jsonify({"error": err_msg}), 500
