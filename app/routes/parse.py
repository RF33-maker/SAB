from flask import Blueprint, request, jsonify
from app.utils.chat_data import supabase
from app.utils.json_parser import run_from_excel
from app.utils.advanced_team_stats import compute_team_advanced, fetch_team_stats_for_league
import traceback
import logging
import io

parse_bp = Blueprint("parse", __name__)
log = logging.getLogger("parse")


@parse_bp.route("/api/parse", methods=["POST"])
def handle_parse():
    try:
        data = request.get_json()
        if not data:
            log.warning("Request did not include valid JSON.")
            return jsonify({"error": "Missing or invalid JSON in request body"}), 400

        file_path = data.get("file_path")
        user_id = data.get("user_id")

        if not file_path or not user_id:
            log.warning("Missing file_path or user_id")
            return jsonify({"error": "file_path and user_id are required"}), 400

        log.info("Parsing Excel file for user=%s path=%s", user_id, file_path)

        try:
            league_id = run_from_excel(file_path, user_id)
            log.info("Excel parse complete: %s", file_path)
            
            if league_id:
                log.info("Computing advanced team stats for league_id=%s", league_id)
                try:
                    team_rows = fetch_team_stats_for_league(league_id)
                    if team_rows:
                        log.info("Found %d team stat records for league %s", len(team_rows), league_id)
                        processed = compute_team_advanced(team_rows)
                        log.info("Computed advanced stats for %d teams", processed)
                    else:
                        log.warning("No team stats found for league_id %s", league_id)
                except Exception as adv_err:
                    log.error("Advanced stats calculation error: %s", adv_err, exc_info=True)
            else:
                log.warning("No league_id returned from Excel parser — advanced stats skipped")
            
            return jsonify({
                "status": "success",
                "message": f"Excel file {file_path} parsed and stored successfully"
            })

        except Exception as e:
            log.error("Excel parse error: %s", e, exc_info=True)
            return jsonify({"error": f"Parse failed: {str(e)}"}), 500

    except Exception as e:
        log.error("Fatal error in /api/parse: %s", e, exc_info=True)
        return jsonify({"error": f"Fatal error: {str(e)}"}), 500
