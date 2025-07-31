from flask import Blueprint, request, jsonify
from app.utils.chat_data import supabase
from app.utils.parser import extract_player_stats
from app.utils.summary import generate_game_summary
import traceback
from datetime import datetime
import io
import fitz

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
            print("❌ Request did not include valid JSON.")
            return jsonify({"error": "Missing or invalid JSON in request body"}), 400

        file_path = data.get("file_path")
        user_id = data.get("user_id")
        league_id = data.get("league_id")

        if not file_path or not user_id or not league_id:
            print("❌ Missing file_path or user_id or league_id")
            return jsonify({"error": "file_path, user_id and league_id are required"}), 400

        print(f"[📥] Parsing file for user: {user_id}")
        print(f"[📄] File path: {file_path}")

        try:
            response = supabase.storage.from_("user-uploads").download(file_path)
        except Exception as e:
            print(f"❌ Supabase threw an exception: {e}")
            return jsonify({"error": f"Supabase threw: {str(e)}"}), 500

        if not response:
            print("❌ Supabase returned empty response — file likely missing or blocked")
            return jsonify({"error": "Supabase returned empty response"}), 404

        print("✅ Supabase file response received")

        try:
            pdf_file = io.BytesIO(response)
        except Exception as e:
            print(f"❌ Writing to local file failed: {e}")
            return jsonify({"error": f"Write failed: {str(e)}"}), 500

        # 👇 Parse the PDF and return both players and game info
        try:
            result = extract_player_stats(pdf_file, league_id)
        except Exception as e:
            print(f"🚨 extract_player_stats error: {e}")
            return jsonify({"error": f"Parse failed: {str(e)}"}), 500

        players = result.get("players", [])
        game = result.get("game", {})

        print(f"✅ Parsed player count: {len(players)}")
        if not players:
            print("❌ No players returned from parser.")
            return jsonify({"error": "No player data extracted from PDF"}), 400

        # 📦 Add metadata to player rows
        for player in players:
            player["user_id"] = user_id
            player["league_id"] = league_id
            player["is_public"] = True
            player["is_home_player"] = player["team"] == game["home_team"]


        # 📦 Add metadata to game row
        game["league_id"] = league_id
        game["pdf_url"] = file_path

        # ✅ Insert game first
        try:
            game_response = supabase.table("games").insert(game).execute()
            print(f"📦 Game insert response: {game_response}")
        except Exception as e:
            print(f"❌ Game insert failed: {e}")
            return jsonify({"error": f"Game insert failed: {str(e)}"}), 500

        # ✅ Then insert players
        try:
            insert_response = supabase.table("player_stats").insert(players).execute()
            print(f"📥 Player insert response: {insert_response}")
        except Exception as e:
            print(f"❌ Player insert failed: {e}")
            return jsonify({"error": f"Player insert failed: {str(e)}"}), 500

        print(f"🎯 Successfully inserted {len(players)} players and 1 game.")

        # 🎯 Generate AI Summary
        try:
            # Step 1: Get top players (by points)
            top_players = sorted(players, key=lambda p: p.get("points", 0), reverse=True)[:3]

            # Step 2: Build the game_data dict for summary
            game_data = {
                "home_team": game.get("home_team"),
                "away_team": game.get("away_team"),
                "home_score": game.get("home_score"),
                "away_score": game.get("away_score"),
                "game_date": game.get("game_date"),
                "top_players": top_players
            }

            # Step 3: Extract text from first page of the PDF
            def extract_first_page_text(pdf_file_obj):
                doc = fitz.open(stream=pdf_file_obj, filetype="pdf")
                text = doc[0].get_text()
                doc.close()
                return text

            pdf_file.seek(0)
            pdf_text = extract_first_page_text(pdf_file)

            # Step 4: Generate and store summary
            generate_game_summary(game_data, pdf_text)
            print("✅ AI Summary generated and saved.")

        except Exception as summary_error:
            print(f"⚠️ Failed to generate AI summary: {summary_error}")

        return jsonify({
            "status": "success",
            "records_added": len(players),
            "example": players[0],
            "game_id": game.get("id")
        })

    except Exception as e:
        traceback.print_exc()
        print(f"🔥 Fatal error in /api/parse: {str(e)}")
        return jsonify({"error": f"Fatal error: {str(e)}"}), 500
