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

        # Step 1: Download from Supabase
        try:
            response = supabase.storage.from_("user-uploads").download(file_path)
        except Exception as e:
            log_to_file(f"❌ Supabase threw an exception: {e}")
            return jsonify({"error": f"Supabase threw: {str(e)}"}), 500

        if not response:
            log_to_file("❌ Supabase returned empty response — file likely missing or blocked")
            return jsonify({"error": "Supabase returned empty response"}), 404

        log_to_file("✅ Supabase file response received")

        # Step 2: Read into memory
        try:
            pdf_file = io.BytesIO(response)
        except Exception as e:
            log_to_file(f"❌ Writing to local file failed: {e}")
            return jsonify({"error": f"Write failed: {str(e)}"}), 500

        # Step 3: Parse PDF
        try:
            result = extract_player_stats(pdf_file, league_id)
        except Exception as e:
            log_to_file(f"🚨 extract_player_stats error: {e}")
            return jsonify({"error": f"Parse failed: {str(e)}"}), 500

        players = result.get("players", [])

        log_to_file(f"✅ Parsed player count: {len(players)}")
        if not players:
            log_to_file("❌ No players returned from parser.")
            return jsonify({"error": "No player data extracted from PDF"}), 400

        log_to_file("🧪 Player example:\n" + json.dumps(players[0], indent=2))

        # Step 4: Add metadata
        for player in players:
            player["user_id"] = user_id
            player["league_id"] = league_id
            player["is_public"] = True

        # Step 5: Insert into Supabase
        try:
            insert_response = supabase.table("player_stats").insert(players).execute()
            log_to_file(f"📥 Insert response: {insert_response}")
        except Exception as e:
            log_to_file(f"❌ Supabase insert failed: {e}")
            return jsonify({"error": f"Insert failed: {str(e)}"}), 500

        if getattr(insert_response, "error", None):
            raise Exception(insert_response.error)

        log_to_file(f"🎯 Successfully inserted {len(players)} records.")
        return jsonify({
            "status": "success",
            "records_added": len(players),
            "example": players[0]
        })

    except Exception as e:
        err_msg = f"🔥 Fatal error in /api/parse: {str(e)}"
        log_to_file(err_msg)
        traceback.print_exc()
        return jsonify({"error": err_msg}), 500
