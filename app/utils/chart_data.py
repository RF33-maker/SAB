from statistics import mean
from app.utils.chat_data import fetch_player_records

KEY_STATS = [
    "points", "assists", "rebounds_total",
    "turnovers", "steals", "blocks",
    "field_goal_percent", "three_pt_percent", "free_throw_percent"
]

def get_stat_summary_for_chart(player_name: str) -> list:
    records = fetch_player_records(player_name)
    if not records or len(records) < 1:
        return []

    summary = []
    last = records[0]
    prev = records[1] if len(records) > 1 else {}

    for stat in KEY_STATS:
        last_val = last.get(stat, 0)
        prev_val = prev.get(stat, 0)
        avg_vals = [r.get(stat, 0) for r in records if isinstance(r.get(stat), (int, float))]

        if not avg_vals:
            continue

        summary.append({
            "stat": stat.replace("_", " ").title(),
            "last_game": round(last_val, 2),
            "previous_game": round(prev_val, 2) if prev else None,
            "average": round(mean(avg_vals), 2)
        })

    return summary
