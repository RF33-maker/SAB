from flask import Flask, jsonify, request
from flask_cors import CORS
from app.routes.parse import parse_bp
from app.routes.query import query_bp
from app.routes.chart import chart_bp
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

if __name__ == "__main__":
    import logging
    logging.basicConfig(level=logging.DEBUG)
    print("🚀 Flask app is running...")
    flask_app.run(host="0.0.0.0", port=5000, debug=True)

