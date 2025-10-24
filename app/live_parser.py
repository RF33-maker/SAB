
import time
import requests
from app.utils.supabase_client import supabase

POLL_INTERVAL = 10  # seconds

def fetch_active_games():
    """Return all games that have a LiveStats data.json link and might be live."""
    res = supabase.table("games").select("id, pdf_url, game_date").execute()
    return [
        g for g in res.data
        if g["pdf_url"] and "data.json" in g["pdf_url"]
    ]

def process_game(game):
    game_id = game["id"]
    url = game["pdf_url"]
    try:
        data = requests.get(url, timeout=10).json()
        plays = data.get("Plays") or data.get("plays") or []

        if not plays:
            print(f"⚠️ No plays yet for {game_id}")
            return

        new_plays, shots = [], []

        for p in plays:
            play = {
                "game_id": game_id,
                "action_number": p.get("actionNumber"),
                "period": p.get("period"),
                "clock": p.get("clock"),
                "player_name": p.get("player"),
                "team_no": p.get("tno"),
                "action_type": p.get("actionType"),
                "sub_type": p.get("subType"),
                "qualifiers": p.get("qualifier", []),
                "success": bool(p.get("success")),
                "scoring": bool(p.get("scoring")),
                "score": f"{p.get('s1')}-{p.get('s2')}",
                "x_coord": p.get("x"),
                "y_coord": p.get("y"),
            }
            new_plays.append(play)

            if p.get("actionType") in ["2pt", "3pt", "freethrow"]:
                shots.append({
                    "game_id": game_id,
                    "player_name": p.get("player"),
                    "team_no": p.get("tno"),
                    "period": p.get("period"),
                    "clock": p.get("clock"),
                    "shot_type": p.get("actionType"),
                    "sub_type": p.get("subType"),
                    "success": bool(p.get("success")),
                    "x": p.get("x"),
                    "y": p.get("y"),
                })

        if new_plays:
            supabase.table("live_events").upsert(new_plays, on_conflict="action_number").execute()
            print(f"✅ {len(new_plays)} plays synced for {game_id}")

        if shots:
            supabase.table("shot_chart").upsert(
                shots,
                on_conflict=["game_id","player_name","period","clock"]
            ).execute()
            print(f"🎯 {len(shots)} shots synced for {game_id}")

    except Exception as e:
        print(f"❌ Error processing {game_id}: {e}")

def run_loop():
    print("🏀 Live parser started")
    while True:
        games = fetch_active_games()
        if not games:
            print("⚠️ No active games found")
        for g in games:
            process_game(g)
        time.sleep(POLL_INTERVAL)

if __name__ == "__main__":
    run_loop()
