from flask import Blueprint, request, jsonify
from app.utils.chat_data import supabase
from app.utils.json_parser import run_from_excel
from app.utils.advanced_team_stats import compute_team_advanced, fetch_team_stats_for_league
import traceback
import logging
import io

parse_bp = Blueprint("parse", __name__)
log = logging.getLogger("parse")


@parse_bp.route("/api/parse-pdf", methods=["POST"])
def handle_parse_pdf():
    """
    Ingest a Genius Sports post-game PDF.

    Accepts multipart/form-data with:
      - file:         PDF file (required)
      - league_name:  Competition / league name (required)
      - game_key:     Override game_key (optional — defaults to PDF_{game_no})
      - user_id:      User UUID for entity tracking (optional)

    Returns JSON with parse result including report_type, game_key, counts.
    """
    try:
        from app.utils.pdf_parser import parse_pdf

        if "file" not in request.files:
            return jsonify({"error": "No PDF file provided (use form field 'file')"}), 400

        pdf_file = request.files["file"]
        if not pdf_file.filename or not pdf_file.filename.lower().endswith(".pdf"):
            return jsonify({"error": "Uploaded file must be a PDF"}), 400

        league_name = request.form.get("league_name", "").strip()
        if not league_name:
            return jsonify({"error": "league_name is required"}), 400

        game_key = request.form.get("game_key", "").strip() or None
        user_id = request.form.get("user_id", "").strip() or None

        log.info(
            "PDF parse request: file=%s league=%s game_key=%s user=%s",
            pdf_file.filename, league_name, game_key, user_id,
        )

        result = parse_pdf(
            pdf_file=pdf_file.stream,
            league_name=league_name,
            provided_game_key=game_key,
            user_id=user_id,
        )

        if "error" in result:
            log.error("PDF parse error: %s", result["error"])
            return jsonify(result), 500

        return jsonify({"status": "success", **result})

    except Exception as e:
        log.error("Fatal error in /api/parse-pdf: %s", e, exc_info=True)
        return jsonify({"error": f"Fatal error: {str(e)}"}), 500


@parse_bp.route("/api/parse", methods=["POST"])
def handle_parse():
    try:
        data = request.get_json()
        if not data:
            log.warning("Request did not include valid JSON.")
            return jsonify({"error": "Missing or invalid JSON in request body"}), 400

        file_path = data.get("file_path")
        user_id = data.get("user_id")
        league_name = data.get("league_name", "").strip() or None

        if not file_path or not user_id:
            log.warning("Missing file_path or user_id")
            return jsonify({"error": "file_path and user_id are required"}), 400

        # Auto-detect PDF by downloading and checking magic bytes
        file_bytes = None
        try:
            bucket, filename = file_path.split("/", 1)
            file_bytes = supabase.storage.from_(bucket).download(filename)
        except Exception:
            pass  # fall through to run_from_excel which handles local paths too

        if file_bytes and file_bytes[:4] == b"%PDF":
            log.info("PDF detected in /api/parse — routing to PDF parser: %s", file_path)
            from app.utils.pdf_parser import parse_pdf
            import io
            result = parse_pdf(
                pdf_file=io.BytesIO(file_bytes),
                league_name=league_name or "Unknown",
                user_id=user_id,
            )
            if "error" in result:
                log.error("PDF parse error: %s", result["error"])
                return jsonify(result), 500
            return jsonify({"status": "success", **result})

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
