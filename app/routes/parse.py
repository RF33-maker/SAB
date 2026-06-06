from flask import Blueprint, request, jsonify
from app.utils.chat_data import supabase
from app.utils.json_parser import run_from_excel
from app.utils.pdf_parser import parse_pdf, parse_pdf_header_only, _parse_competition_components
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
      - competition_name:  Competition / league name (required)
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

        league_name = (
            request.form.get("competition_name", "").strip()
            or request.form.get("league_name", "").strip()
        )
        if not league_name:
            return jsonify({"error": "competition_name is required"}), 400

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
        league_name = (
            data.get("competition_name", "").strip()
            or data.get("league_name", "").strip()
            or None
        )

        if not file_path or not user_id:
            log.warning("Missing file_path or user_id")
            return jsonify({"error": "file_path and user_id are required"}), 400

        # Auto-detect PDF by downloading and checking magic bytes
        file_bytes = None
        try:
            bucket, filename = file_path.split("/", 1)
            file_bytes = supabase.storage.from_(bucket).download(filename)
        except Exception as download_err:
            log.warning("Supabase storage download failed for %s: %s", file_path, download_err)
            return jsonify({"error": f"Storage download failed: {str(download_err)}"}), 500

        if file_bytes and file_bytes[:4] == b"%PDF":
            log.info("PDF detected in /api/parse — routing to PDF parser: %s", file_path)
            result = parse_pdf(
                pdf_file=io.BytesIO(file_bytes),
                league_name=league_name or "Unknown",
                user_id=user_id,
            )

            if "error" in result:
                log.error("PDF parse error: %s", result["error"])
                return jsonify({"status": "error", **result}), 500

            if result.get("skipped"):
                report_type = result.get("report_type", "unknown")
                msg = result.get("message", "PDF was skipped")
                log.warning("PDF skipped — type=%s file=%s reason=%s", report_type, file_path, msg)
                return jsonify({
                    "status": "skipped",
                    "report_type": report_type,
                    "message": msg,
                }), 200

            report_type = result.get("report_type", "?")
            counts = result.get("counts", {})
            log.info(
                "PDF parsed OK — type=%s game_key=%s counts=%s",
                report_type, result.get("game_key"), counts,
            )
            return jsonify({"status": "success", **result})

        log.info("Parsing Excel file for user=%s path=%s", user_id, file_path)

        try:
            result = run_from_excel(file_path, user_id)
            log.info("Excel parse complete: %s — %s", file_path, result)

            league_id = result.get("league_id") if isinstance(result, dict) else result
            processed = result.get("processed", 0) if isinstance(result, dict) else 0
            skipped = result.get("skipped", 0) if isinstance(result, dict) else 0
            errors = result.get("errors", 0) if isinstance(result, dict) else 0
            total_rows = result.get("total_rows", 0) if isinstance(result, dict) else 0

            if skipped > 0 and processed == 0:
                log.warning(
                    "All %d rows skipped (unchanged) — no new data written for %s",
                    skipped, file_path,
                )

            return jsonify({
                "status": "success",
                "message": f"Excel file parsed: {processed} processed, {skipped} skipped (unchanged), {errors} errors out of {total_rows} rows",
                "processed": processed,
                "skipped": skipped,
                "errors": errors,
                "total_rows": total_rows,
                "league_id": league_id,
            })

        except Exception as e:
            log.error("Excel parse error: %s", e, exc_info=True)
            return jsonify({"error": f"Parse failed: {str(e)}"}), 500

    except Exception as e:
        log.error("Fatal error in /api/parse: %s", e, exc_info=True)
        return jsonify({"error": f"Fatal error: {str(e)}"}), 500


@parse_bp.route("/api/parse/scan", methods=["POST"])
def handle_scan():
    """
    Read a PDF from Supabase storage and return its metadata WITHOUT writing
    anything to the database. Used by the frontend to pre-fill the upload form
    (parent league, age group, round, teams, report type, etc.).

    Request JSON: { file_path, user_id }
    Response JSON: { report_type, game_key, competition, parent_league,
                     age_group, round_name, home_team, away_team,
                     home_score, away_score, game_date, venue }
    """
    try:
        data = request.get_json()
        if not data:
            return jsonify({"error": "Missing JSON body"}), 400

        file_path = data.get("file_path")
        if not file_path:
            return jsonify({"error": "file_path is required"}), 400

        try:
            bucket, filename = file_path.split("/", 1)
            file_bytes = supabase.storage.from_(bucket).download(filename)
        except Exception as dl_err:
            return jsonify({"error": f"Storage download failed: {dl_err}"}), 500

        if not file_bytes or file_bytes[:4] != b"%PDF":
            return jsonify({"error": "File is not a valid PDF"}), 400

        result = parse_pdf_header_only(io.BytesIO(file_bytes))
        if "error" in result:
            return jsonify(result), 500
        return jsonify(result)

    except Exception as e:
        log.error("Fatal error in /api/parse/scan: %s", e, exc_info=True)
        return jsonify({"error": f"Fatal error: {str(e)}"}), 500


@parse_bp.route("/api/leagues", methods=["GET"])
def list_leagues():
    """
    Return all leagues with their auto-parsed components (parent_league,
    age_group, round_name). The frontend uses this to build a smart
    parent-league dropdown instead of showing the raw full league name.

    Query params:
      parent_name  — filter by parent league name (optional, case-insensitive)

    Response JSON:
    {
      "leagues": [
        {
          "league_id": "...",
          "name": "REBA Summer League 14U Stop 3 FIBA",
          "parent_league": "REBA Summer League FIBA",
          "age_group": "14U",
          "round_name": "Stop 3"
        }, ...
      ],
      "parents": ["REBA Summer League FIBA", "WEABL 2025-26", ...]
    }
    """
    try:
        parent_filter = (request.args.get("parent_name") or "").strip().lower()

        # Paginate through all leagues (PostgREST default limit 1000)
        all_leagues = []
        offset = 0
        page_size = 1000
        while True:
            res = supabase.table("competitions") \
                .select("league_id, name, slug, created_by") \
                .range(offset, offset + page_size - 1) \
                .execute()
            batch = res.data or []
            all_leagues.extend(batch)
            if len(batch) < page_size:
                break
            offset += page_size

        enriched = []
        parent_set = set()
        for row in all_leagues:
            name = row.get("name") or ""
            components = _parse_competition_components(name)
            parent = components["parent_league"]
            parent_set.add(parent)
            if parent_filter and parent_filter not in parent.lower():
                continue
            enriched.append({
                "league_id": row["league_id"],
                "name": name,
                "slug": row.get("slug"),
                "parent_league": parent,
                "age_group": components["age_group"],
                "round_name": components["round_name"],
            })

        # Sort: parent_league → age_group → round_name
        enriched.sort(key=lambda x: (
            x["parent_league"] or "",
            x["age_group"] or "",
            x["round_name"] or "",
        ))

        return jsonify({
            "leagues": enriched,
            "parents": sorted(parent_set),
            "total": len(enriched),
        })

    except Exception as e:
        log.error("Fatal error in /api/leagues: %s", e, exc_info=True)
        return jsonify({"error": f"Fatal error: {str(e)}"}), 500
