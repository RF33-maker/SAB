from openai import OpenAI
from app.utils.chat_data import supabase
from datetime import datetime

client = OpenAI()

print("📄 summary.py loaded")

def generate_game_summary(game, players):
    try:
        home = game['home_team']
        away = game['away_team']
        home_players = [p for p in players if p['team'] == home]
        away_players = [p for p in players if p['team'] == away]

        def sum_stat(stat, team):
            return sum([p.get(stat, 0) for p in team if p.get(stat) is not None])

        def efg(team):
            fg_made = sum_stat("field_goals_made", team)
            three_made = sum_stat("three_pt_made", team)
            fg_att = sum_stat("field_goals_attempted", team)
            return round(((fg_made + 0.5 * three_made) / fg_att) * 100, 1) if fg_att else 0

        def ft_rate(team):
            ft_made = sum_stat("free_throws_made", team)
            fga = sum_stat("field_goals_attempted", team)
            return round((ft_made / fga) * 100, 1) if fga else 0

        def rebounds(team):
            return sum_stat("rebounds_total", team)

        def get_top_players(team):
            sorted_team = sorted(team, key=lambda x: x.get("points", 0), reverse=True)
            return sorted_team[:2]

        top_home = get_top_players(home_players)
        top_away = get_top_players(away_players)

        payload = {
            "home_team": home,
            "away_team": away,
            "home_score": game['home_score'],
            "away_score": game['away_score'],
            "date": game['game_date'],
            "four_factors": {
                "eFG%": {"home": efg(home_players), "away": efg(away_players)},
                "Turnovers": {"home": sum_stat("turnovers", home_players), "away": sum_stat("turnovers", away_players)},
                "Rebounds": {"home": rebounds(home_players), "away": rebounds(away_players)},
                "FT Rate": {"home": ft_rate(home_players), "away": ft_rate(away_players)}
            },
            "key_players": {
                home: [{"name": p['name'], "points": p['points'], "rebounds": p['rebounds_total']} for p in top_home],
                away: [{"name": p['name'], "points": p['points'], "rebounds": p['rebounds_total']} for p in top_away]
            }
        }

        system_msg = """
You are a basketball analyst. Generate a short, clean game summary using the Four Factors:
- Shooting (eFG%)
- Turnovers
- Rebounding
- Free Throws (FT %)

Stick to this structure:
1. Final Score (one line)
2. Key Stats (bullet points, each with stat values)
3. Key Players (brief)

Be objective and concise. Focus on cause-and-effect relationships in the stats, not opinions or filler.
"""

        user_msg = f"Here is the game data:\n\n{payload}"

        response = client.chat.completions.create(
            model="gpt-4",
            messages=[
                {"role": "system", "content": system_msg},
                {"role": "user", "content": user_msg}
            ]
        )

        summary = response.choices[0].message.content.strip()

        print("📤 Inserting summary to Supabase with payload:")

        # ✅ Save to Supabase
        supabase.table("summaries").insert({
            "summary_type": "game",
            "ref_id": f"{game['home_team']}_{game['away_team']}_{game['game_date']}".lower().replace(" ", "_"),
            "content": summary,
            "team": game['home_team'],
            "game_date": game['game_date'].isoformat() if hasattr(game['game_date'], 'isoformat') else str(game['game_date'])

        }).execute()
        print("✅ Summary inserted into Supabase")

        return summary

    except Exception as e:
        return f"⚠️ Failed to generate AI summary: {e}"
