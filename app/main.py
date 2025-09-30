from flask import Flask, jsonify, request
from flask_cors import CORS
from app.routes.parse import parse_bp
from app.routes.query import query_bp
from app.routes.chart import chart_bp
from app.utils.json_parser import run_from_excel
import openai
import os

print("✅ App started, importing blueprints...")

flask_app = Flask(__name__)
CORS(flask_app, resources={r"/*": {"origins": "*"}}, supports_credentials=True)

flask_app.register_blueprint(parse_bp)
flask_app.register_blueprint(query_bp)
flask_app.register_blueprint(chart_bp)

@flask_app.route('/')
def home():
    return {"status": "ok", "message": "Stats processor ready"}

@flask_app.route('/test_chart_data')
def test_chart_data():
    return jsonify([
        {"stat": "Points", "last_game": 20, "previous_game": 18, "average": 19},
        {"stat": "Assists", "last_game": 4, "previous_game": 6, "average": 5}
    ])

@flask_app.route("/api/parse", methods=["POST"])
def parse_file():
    data = request.get_json()

    if not data or "file_path" not in data:
        return {"status": "error", "message": "Missing file_path"}, 400

    file_path = data["file_path"]  # e.g. "uploads/my_games.xlsx"
    print(f"📂 Received parse request for: {file_path}")

    try:
        run_from_excel(file_path)   # 🔥 this calls your parser
        return {"status": "ok", "file": file_path}, 200
    except Exception as e:
        print(f"❌ Parser failed: {e}")
        return {"status": "error", "message": str(e)}, 500

if __name__ == "__main__":
    import logging
    logging.basicConfig(level=logging.DEBUG)
    print("🚀 Flask app is running...")
    flask_app.run(host="0.0.0.0", port=5000, debug=False)

