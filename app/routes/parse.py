from flask import Blueprint, request, jsonify
from app.utils.chat_data import supabase
from app.utils.json_parser import run_from_excel
from app.utils.advanced_team_stats import compute_team_advanced, fetch_team_stats_for_league
import traceback
from datetime import datetime
import io

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

        if not file_path or not user_id:
            print("❌ Missing file_path or user_id")
            return jsonify({"error": "file_path and user_id are required"}), 400

        print(f"[📥] Parsing Excel file for user: {user_id}")
        print(f"[📄] File path: {file_path}")

        try:
            league_id = run_from_excel(file_path, user_id)
            print(f"✅ Successfully parsed Excel file: {file_path}")
            
            # Compute advanced team stats if we have a league_id
            if league_id:
                print(f"\n📊 Computing advanced team stats for league: {league_id}")
                try:
                    team_rows = fetch_team_stats_for_league(league_id)
                    if team_rows:
                        print(f"   Found {len(team_rows)} team stat records")
                        processed = compute_team_advanced(team_rows)
                        print(f"   ✅ Computed advanced stats for {processed} teams")
                    else:
                        print(f"   ⚠️  No team stats found for league {league_id}")
                except Exception as adv_err:
                    print(f"   ⚠️  Advanced stats calculation error: {adv_err}")
                    # Don't fail the entire upload if advanced stats fail
            else:
                print("   ⚠️  No league_id detected, skipping advanced stats")
            
            return jsonify({
                "status": "success",
                "message": f"Excel file {file_path} parsed and stored successfully"
            })

        except Exception as e:
            print(f"🚨 Excel parse error: {e}")
            traceback.print_exc()
            return jsonify({"error": f"Parse failed: {str(e)}"}), 500

    except Exception as e:
        traceback.print_exc()
        print(f"🔥 Fatal error in /api/parse: {str(e)}")
        return jsonify({"error": f"Fatal error: {str(e)}"}), 500
