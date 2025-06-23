# app/routes/chart.py

from flask import Blueprint, jsonify
import asyncio
from app.utils.chat_data import supabase  # ✅ this is the key fix
from app.utils.chart_data import get_stat_summary_for_chart

chart_bp = Blueprint("chart_bp", __name__)

@chart_bp.route("/players", methods=["GET"])
def list_players():
    print("🟡 /players route hit")

    try:
        result = supabase.table("player_stats").select("name").execute()
        print("✅ Supabase result:", result.data)
    except Exception as e:
        print("❌ Supabase fetch error:", str(e))
        return jsonify({"error": str(e)}), 500

    names = sorted(set(row["name"] for row in result.data if row.get("name")))
    print("✅ Final player list:", names)
    return jsonify(names)

@chart_bp.route("/chart_summary/<player_name>", methods=["GET"])
def chart_summary(player_name):
    print(f"📊 Generating summary for {player_name}")
    try:
        
        summary = get_stat_summary_for_chart(player_name)
        return jsonify(summary)
    except Exception as e:
        print("❌ Chart summary error:", e)
        return jsonify({"error": str(e)}), 500