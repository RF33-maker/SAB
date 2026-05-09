from flask import Flask, jsonify, request
from flask_cors import CORS
from app.routes.parse import parse_bp
from app.routes.chart import chart_bp
from app.routes.lineups import lineups_bp
from app.routes.admin import admin_bp
from app.utils.json_parser import run_from_excel
import os
import logging
import sys

LOG_LEVEL = os.environ.get("LOG_LEVEL", "WARNING").upper()
logging.basicConfig(
    level=getattr(logging, LOG_LEVEL, logging.WARNING),
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
    stream=sys.stdout,
)
log = logging.getLogger("app")
log.warning("Swish Assistant is live.")

ENABLE_OPENAI = os.environ.get("ENABLE_OPENAI", "false").lower() == "true"

flask_app = Flask(__name__)

ALLOWED_ORIGINS = [
    "https://swishassistant.com",
    "https://www.swishassistant.com",
    "http://localhost:5173",
    "http://localhost:3000",
]

CORS(
    flask_app,
    resources={r"/api/*": {"origins": ALLOWED_ORIGINS}},
    supports_credentials=True,
    allow_headers=["Content-Type", "Authorization", "X-Admin-Key"],
    methods=["GET", "POST", "PUT", "PATCH", "DELETE", "OPTIONS"],
    expose_headers=["Content-Type"],
    max_age=600,
)
log.warning(
    "CORS configured for flask_app (/api/*) — allowed origins: %s",
    ", ".join(ALLOWED_ORIGINS),
)

flask_app.register_blueprint(parse_bp)
flask_app.register_blueprint(chart_bp)
flask_app.register_blueprint(lineups_bp)
flask_app.register_blueprint(admin_bp)

if ENABLE_OPENAI:
    from app.routes.query import query_bp
    flask_app.register_blueprint(query_bp)
    log.info("OpenAI features ENABLED")
else:
    log.info("OpenAI features DISABLED")

    @flask_app.route('/start', methods=['GET', 'OPTIONS'])
    @flask_app.route('/reset', methods=['GET', 'OPTIONS'])
    @flask_app.route('/chat', methods=['POST', 'OPTIONS'])
    @flask_app.route('/check_summary', methods=['POST', 'OPTIONS'])
    @flask_app.route('/api/chat/league', methods=['POST', 'OPTIONS'])
    @flask_app.route('/api/generate-summary', methods=['POST', 'OPTIONS'])
    @flask_app.route('/api/ai-analysis', methods=['POST', 'OPTIONS'])
    def ai_not_enabled():
        if request.method == 'OPTIONS':
            origin = request.headers.get('Origin', '')
            resp = flask_app.make_default_options_response()
            if origin in ALLOWED_ORIGINS:
                resp.headers['Access-Control-Allow-Origin'] = origin
            resp.headers['Access-Control-Allow-Methods'] = 'GET, POST, OPTIONS'
            resp.headers['Access-Control-Allow-Headers'] = 'Content-Type, Authorization'
            resp.headers['Access-Control-Allow-Credentials'] = 'true'
            return resp, 204
        origin = request.headers.get('Origin', '')
        resp = jsonify({"error": "AI features not enabled"})
        if origin in ALLOWED_ORIGINS:
            resp.headers['Access-Control-Allow-Origin'] = origin
        resp.headers['Access-Control-Allow-Credentials'] = 'true'
        return resp, 501

@flask_app.route('/')
def home():
    return {"status": "ok", "message": "Stats processor ready"}

@flask_app.route('/test_chart_data')
def test_chart_data():
    return jsonify([
        {"stat": "Points", "last_game": 20, "previous_game": 18, "average": 19},
        {"stat": "Assists", "last_game": 4, "previous_game": 6, "average": 5}
    ])

if __name__ == "__main__":
    logging.getLogger("httpx").setLevel(logging.WARNING)
    logging.getLogger("httpcore").setLevel(logging.WARNING)
    logging.getLogger("hpack").setLevel(logging.WARNING)
    flask_app.run(host="0.0.0.0", port=5000, debug=False)

