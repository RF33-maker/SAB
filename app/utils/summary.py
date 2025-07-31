from openai import OpenAI
from supabase import create_client
import os

# ✅ Set up Supabase and OpenAI clients
supabase = create_client(
    os.environ["SUPABASE_URL"],
    os.environ["SUPABASE_KEY"]
)

client = OpenAI(api_key=os.environ["OPENAI_API_KEY"])

# ✅ Function to generate and store a game summary
def generate_game_summary(game_data, pdf_text=None):
    # Format player stats
    top_players_section = "\n".join([
        f"- {p['name']} ({p['team']}): {p['points']} pts, {p['rebounds_total']} reb"
        for p in game_data.get('top_players', [])
    ])

    # Optional: add trimmed PDF text (limit to ~1,500 characters)
    additional_context = f"\n\nAdditional context from the official report:\n{pdf_text[:1500]}" if pdf_text else ""

    # 🧠 Construct the prompt
    prompt = f"""Write a short professional recap for a basketball game from the perspective of a high-level coach. 
It should be clear and suitable for a league website, no more than 100 words.

Game Details:
- {game_data['home_team']} {game_data['home_score']} vs {game_data['away_team']} {game_data['away_score']}
- Date: {game_data['game_date']}

Top Players:
{top_players_section}
{additional_context}
"""

    # 🔁 Call OpenAI
    response = client.chat.completions.create(
        model="gpt-4",
        messages=[
            {"role": "system", "content": "You are a basketball journalist."},
            {"role": "user", "content": prompt}
        ]
    )

    content = response.choices[0].message.content if response.choices and response.choices[0].message.content else ""
    summary = content.strip()


    # ✅ Save to Supabase
    supabase.table("summaries").insert({
        "summary_type": "game",
        "ref_id": f"{game_data['home_team']}_{game_data['away_team']}_{game_data['game_date']}".lower().replace(" ", "_"),
        "content": summary,
        "team": game_data['home_team'],
        "game_date": game_data['game_date']
    }).execute()

    return summary
