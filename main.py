
import argparse
import pdfplumber
import pandas as pd
import json
import requests
import os
import requests

def extract_player_stats(pdf_path):
    import pdfplumber, pandas as pd
    import re
    from datetime import datetime

    # Safe getter for multiple possible column names
    def get_val(row, *keys, fallback=0):
        for key in keys:
            val = row.get(key)
            if val not in [None, "", "—"]:
                return val
        return fallback

    # 1. Open PDF and grab all tables
    with pdfplumber.open(pdf_path) as pdf:

        first_page_text = pdf.pages[0].extract_text()
        print(first_page_text)

        # Extract teams from "Team A vs Team B"
        match = re.search(r"^(.*)\s\d+\s[–-]\s\d+\s(.*)$", first_page_text, re.MULTILINE)
        if match:
            team = match.group(1).strip()
            opponent = match.group(2).strip()
        else:
            team = "Unknown Team"
            opponent = "Unknown Opponent"

        # Extract date like "10 March 2024"
        date_match = re.search(r"(\d{1,2}\s+\w+\s+\d{4})", first_page_text)
        if date_match:
            parsed_date = datetime.strptime(date_match.group(1), "%d %b %Y")
            game_date = parsed_date.strftime("%Y-%m-%d")
        else:
            game_date = "1970-01-01"

        # Build Game ID
        game_id = f"{game_date}_{team[:3].upper()}_vs_{opponent[:3].upper()}"
        
        all_tables = []
        for page in pdf.pages:
            for table in page.extract_tables():
                if table:
                    all_tables.append(table)

    # 2. Identify box score table
    stat_keywords = {"Min", "PTS", "Field Goals", "Rebounds", "AS", "TO", "ST", "BS"}
    stat_tables = [t for t in all_tables if any(
        any(cell and any(k in cell for k in stat_keywords) for cell in row)
        for row in t)]
    if not stat_tables:
        raise ValueError("No valid player stats table found.")
    stat_table = max(stat_tables, key=len)
    df = pd.DataFrame(stat_table)

    # 3. Build and fix merged headers
    header1, header2 = df.iloc[0], df.iloc[1]
    headers = []
    for h1, h2 in zip(header1, header2):
        if h1 and h2:
            headers.append(f"{h1.strip()} {h2.strip()}")
        elif h1:
            headers.append(h1.strip())
        elif h2:
            headers.append(h2.strip())
        else:
            headers.append("")

    # Disambiguate the duplicate `%` columns
    for i, col in enumerate(headers):
        if col == "%":
            prev = headers[i-1]
            if "2 Points M/A" in prev:   headers[i] = "2 Points %"
            elif "3 Points M/A" in prev: headers[i] = "3 Points %"
            elif "Free Throws M/A" in prev: headers[i] = "Free Throws %"

    df.columns = headers
    df = df[2:].reset_index(drop=True)
    df = df[df["Min"] != "DNP"]  # drop DNPs

    # 4. Parse each player
    players = []
    for _, row in df.iterrows():
        try:
            # —— True Shooting % (inside the loop!) ——
            fg_made, fg_att = map(int, (row.get("Field Goals M/A","0/0") or "0/0").split("/"))
            ft_made, ft_att = map(int, (row.get("Free Throws M/A","0/0") or "0/0").split("/"))
            pts = int(get_val(row, "PTS"))
            denom = 2 * (fg_att + 0.44 * ft_att)
            ts_pct = round((pts / denom) * 100, 2) if denom > 0 else 0.0

            # Parse 3PM
            three_pm = int((row.get("3 Points M/A", "0/0") or "0/0").split("/")[0])

            # Assists and TO
            assists = int(get_val(row, "AS", "Assists"))
            turnovers = int(get_val(row, "TO", "Turnovers"))

            # 🎯 eFG%
            efg_pct = round(((fg_made + 0.5 * three_pm) / fg_att) * 100, 2) if fg_att > 0 else 0.0

            # 🔁 AST/TO
            ast_to_ratio = round((assists / turnovers), 2) if turnovers > 0 else 0.0


            # Build Unique record ID (Avoid duplicates)
            player_name = row.get("Name", "").strip()
            record_id = f"{game_id}_{player_name.replace(' ', '_')}"

            # Build the player dict
            players.append({
                "number": row.get("No","").strip(),
                "name":   row.get("Name","").strip(),
                "minutes": row.get("Min",""),
                "fg":      row.get("Field Goals M/A",""),
                "fg_pct":  float(get_val(row, "%")),
                "two_pt": {
                    "m_a": row.get("2 Points M/A",""),
                    "pct": float(get_val(row, "2 Points %", fallback=0))
                },
                "three_pt": {
                    "m_a": row.get("3 Points M/A",""),
                    "pct": float(get_val(row, "3 Points %", fallback=0))
                },
                "free_throws": {
                    "m_a": row.get("Free Throws M/A",""),
                    "pct": float(get_val(row, "Free Throws %", fallback=0))
                },
                "rebounds": {
                    "or":  int(get_val(row, "Rebounds OR","OR")),
                    "dr":  int(get_val(row, "DR","Rebounds DR")),
                    "tot": int(get_val(row, "TOT","Rebounds TOT"))
                },
                "assists":   int(get_val(row, "AS","Assists")),
                "turnovers": int(get_val(row, "TO","Turnovers")),
                "assist_turnover_ratio": ast_to_ratio,
                "steals":    int(get_val(row, "ST","Steals")),
                "blocks":    int(get_val(row, "BS","Blocks")),
                "fouls": {
                    "pf": int(get_val(row, "Fouls PF","PF")),
                    "fd": int(get_val(row, "FD"))
                },
                "plus_minus": int(get_val(row, "+/-")),
                "points":     pts,
                "true_shooting_pct": ts_pct,
                "effective_field_goal_pct": efg_pct,
                "game_id": game_id,
                "game_date": game_date,
                "team": team,
                "opponent": opponent,
                "record_id": record_id,
                "effective_field_goal_pct": efg_pct,
                
            })
        except Exception as e:
            print(f"⚠️ Skipping row due to: {e}")
            continue

    return {
        "game_id": "game_test",   # TODO: make this dynamic
        "team":    "Gloucester City Kings",
        "players": players
    }

# Airtable Configuration()

AIRTABLE_API_KEY = os.getenv("AIRTABLE_API_KEY")
BASE_ID          = os.getenv("AIRTABLE_BASE_ID")
TABLE_NAME       = "Player Stats Gloucester Kings"

def push_to_airtable(players):
    url = f"https://api.airtable.com/v0/{BASE_ID}/{TABLE_NAME}"
    print(f"🔗 Airtable URL: {url}")
    print(f"🔑 API key loaded: {'yes' if AIRTABLE_API_KEY else 'NO'}")
    print(f"🏓 About to push {len(players)} records")
    
    headers = {
        "Authorization": f"Bearer {AIRTABLE_API_KEY}",
        "Content-Type":  "application/json"
    }
    
    for player in players:
            # 🔍 Step 1: Check if the record already exists
            record_id = player["record_id"]
            check_url = f"{url}?filterByFormula={{Record ID}}='{record_id}'"
            check_resp = requests.get(check_url, headers=headers)

            # 🧾 Optional debug log
            print(f"🔍 Checking for Record ID: {record_id}")
            print("➡️ GET", check_url)
            print("⬅️ Response:", check_resp.status_code, check_resp.text[:100])

            if check_resp.ok and check_resp.json().get("records"):
                print(f"⚠️ Duplicate found — skipping {player['name']} in {player['game_id']}")
                continue  # 🛑 skip insertion

        
    fields = {
            "Number":                  player["number"],
            "Name":                    player["name"],
            "Minutes Played":          player["minutes"],
            "Field Goals":             player["fg"],
            "Field Goal %":            player["fg_pct"],
            "2 Points M/A":            player["two_pt"]["m_a"],
            "2 Points %":              player["two_pt"]["pct"],
            "3 Points M/A":            player["three_pt"]["m_a"],
            "3 Points %":              player["three_pt"]["pct"],
            "Free Throws M/A":         player["free_throws"]["m_a"],
            "Free Throws %":           player["free_throws"]["pct"],
            "Rebounds O":              player["rebounds"]["or"],
            "Rebounds D":              player["rebounds"]["dr"],
            "Rebounds Total":          player["rebounds"]["tot"],
            "Assists":                 player["assists"],
            "Turnovers":               player["turnovers"],
            "Assist/Turnover Ratio":   player["assist_turnover_ratio"],
            "Steals":                  player["steals"],
            "Blocks":                  player["blocks"],
            "Personal Fouls":          player["fouls"]["pf"],
            "Fouls Drawn":             player["fouls"]["fd"],
            "Plus/Minus":              player["plus_minus"],
            "Points":                  player["points"],
            "True Shooting %":         player["true_shooting_pct"],
            "Effective Field Goal %":  player["effective_field_goal_pct"],
            "Game ID":                 player["game_id"],
            "Game date":               player["game_date"],
            "Team":                    player["team"],
            "Opponent":                player["opponent"],
            "Record ID":               player["record_id"]
            # …other mappings…
        }
    resp = requests.post(url, json={"fields": fields}, headers=headers)
    if not resp.ok:
            print(f"❌ Airtable error for {player['name']}: {resp.text}")


def main():
    import sys
    import os

    parser = argparse.ArgumentParser()
    parser.add_argument("pdf", help="Path to the FIBA LiveStats PDF")
    parser.add_argument("--webhook", help="Optional webhook URL to send the JSON")

    # 👇 Add default if no arguments (for Replit Run button)
    if len(sys.argv) == 1:
        test_pdf = "game_test.pdf"
        if not os.path.exists(test_pdf):
            print(f"❌ Test file '{test_pdf}' not found.")
            return
        sys.argv += [test_pdf]

    args = parser.parse_args()

    data = extract_player_stats(args.pdf)

    # … save JSON, webhook, etc. …
    push_to_airtable(data["players"])

    # Save locally
    output_path = args.pdf.replace(".pdf", ".json")
    with open(output_path, "w") as f:
        json.dump(data, f, indent=2)
    print(f"✅ Saved JSON to {output_path}")

    if args.webhook:
        response = requests.post(args.webhook, json=data)
        print(f"📬 Webhook response: {response.status_code} - {response.text}")


if __name__ == "__main__":
    main()
