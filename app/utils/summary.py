from openai import OpenAI
from supabase import create_client
import os

# ✅ Set up Supabase and OpenAI clients
supabase = create_client(
    os.environ["SUPABASE_URL"],
    os.environ["SUPABASE_SERVICE_ROLE"]
)

client = OpenAI(api_key=os.environ["OPENAI_API_KEY"])

# ✅ Function to generate and store a game summary
def generate_game_summary(game_data):
    prompt = f"""Write a short recap for a basketball game from the perspective of a high level coach, be professional.
- {game_data['home_team']} {game_data['home_score']} vs {game_data['away_team']} {game_data['away_score']} on {game_data['game_date']}.
Top players:
{chr(10).join([f"- {p['name']} ({p['team']}): {p['points']} pts, {p['rebounds']} reb" for p in game_data['top_players']])}
Write it like a league recap, under 100 words."""

    response = client.chat.completions.create(
        model="gpt-4",
        messages=[
            {"role": "system", "content": "You are a basketball journalist."},
            {"role": "user", "content": prompt}
        ]
    )

    summary = response.choices[0].message.content.strip()

    # ✅ Save to Supabase
    supabase.table("summaries").insert({
        "summary_type": "game",
        "ref_id": f"{game_data['home_team']}_{game_data['away_team']}_{game_data['game_date']}".lower().replace(" ", "_"),
        "content": summary,
        "team": game_data['home_team'],
        "game_date": game_data['game_date']
    }).execute()

    return summary


