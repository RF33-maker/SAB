import pdfplumber
import pandas as pd
import re
from datetime import datetime
from app.utils.helpers import get_val
from app.utils.summary import generate_game_summary
from app.utils.chat_data import supabase

print("🐍 parser.py loaded")

def log(msg):
    import sys
    print(msg, file=sys.stdout, flush=True)

def extract_player_stats(pdf_file, league_id):
    print("🛠 Starting PDF parse...")

    try:
        with pdfplumber.open(pdf_file) as pdf:
            if not pdf.pages:
                raise ValueError("❌ PDF has no pages.")

            first_page_text = pdf.pages[0].extract_text()
            print(f"📝 First page:\n{first_page_text}")

            score_match = re.search(r"^(.*)\s(\d+)\s[–-]\s(\d+)\s(.*)$", first_page_text, re.MULTILINE)
            if score_match:
                team = score_match.group(1).strip()
                home_score = int(score_match.group(2))
                away_score = int(score_match.group(3))
                opponent = score_match.group(4).strip()
            else:
                team = "Unknown Team"
                opponent = "Unknown Opponent"
                home_score = 0
                away_score = 0

            home_team = team
            away_team = opponent

            date_match = re.search(r"(\d{1,2}\s+\w+\s+\d{4})", first_page_text)
            game_date = (
                datetime.strptime(date_match.group(1), "%d %b %Y").strftime("%Y-%m-%d")
                if date_match else "1970-01-01"
            )

            venue_match = re.search(r"^(.*?)\s-\sCourt", first_page_text, re.MULTILINE)
            venue = venue_match.group(1).strip() if venue_match else "Unknown Venue"

            game_id = f"{game_date}_{team[:3].upper()}_vs_{opponent[:3].upper()}"

            all_tables = []
            for page in pdf.pages:
                tables = page.extract_tables()
                if tables:
                    all_tables.extend(tables)

            print(f"📑 Extracted {len(all_tables)} tables")
    except Exception as e:
        print(f"❌ Failed to open or read PDF: {e}")
        raise

    if not all_tables:
        raise ValueError("❌ No tables found in the PDF.")

    stat_keywords = {"Min", "PTS", "Field Goals", "Rebounds", "AS", "TO", "ST", "BS"}
    stat_tables = [t for t in all_tables if any(
        any(cell and any(k in str(cell) for k in stat_keywords) for cell in row)
        for row in t)]

    if not stat_tables:
        raise ValueError("❌ No stat tables matched expected keywords.")

    players = []

    for i, stat_table in enumerate(stat_tables):
        if len(stat_table) < 3:
            print("⚠️ Skipping table — not enough rows to build header + data")
            continue

        df = pd.DataFrame(stat_table)
        header1 = df.iloc[0].fillna("").astype(str)
        header2 = df.iloc[1].fillna("").astype(str)

        headers = []
        for h1, h2 in zip(header1, header2):
            combined = f"{h1} {h2}".strip()
            headers.append(combined or h1 or h2)

        print(f"🧩 Flattened headers:\n{headers}")

        if "Min" not in headers:
            print("⚠️ Skipping table — 'Min' column not found")
            continue

        for j, col in enumerate(headers):
            if col == "%":
                prev = headers[j - 1]
                if "2 Points" in prev: 
                    headers[j] = "2 Points %"
                elif "3 Points" in prev: 
                    headers[j] = "3 Points %"
                elif "Free Throws" in prev: 
                    headers[j] = "Free Throws %"
                else: 
                    headers[j] = f"{prev} %"

        df.columns = headers
        df = df[2:].reset_index(drop=True)
        df = df[df["Min"] != "DNP"]

        team_name = team if i == 0 else opponent
        opponent_name = opponent if i == 0 else team

        for _, row in df.iterrows():
            try:
                print(f"👤 Checking row: {row.to_dict()}")
                player_name = (row.get("name") or row.get("Name") or "").strip()
                jersey = str(row.get("No") or "")
                if not player_name or "Totals" in jersey or "Coach" in jersey:
                    print("⛔ Skipping row — missing player_name or unwanted No")
                    continue

                def parse_made_attempts(val):
                    try:
                        return tuple(map(int, val.split("/")))
                    except:
                        return (0, 0)

                def parse_int(val):
                    try:
                        return int(val)
                    except:
                        return 0

                fg_makes, fg_attempts = parse_made_attempts(row.get("Field Goals M/A", "0/0"))
                ft_makes, ft_atts = parse_made_attempts(row.get("Free Throws M/A", "0/0"))
                two_makes, two_atts = parse_made_attempts(row.get("2 Points M/A", "0/0"))
                three_makes, three_atts = parse_made_attempts(row.get("3 Points M/A", "0/0"))

                pts = parse_int(get_val(row, "PTS"))
                efg = ((fg_makes + 0.5 * three_makes) / fg_attempts) * 100 if fg_attempts else 0
                ts_denom = 2 * (fg_attempts + 0.44 * ft_atts)
                ts_pct = (pts / ts_denom) * 100 if ts_denom else 0
                assists = parse_int(get_val(row, "AS"))
                turnovers = parse_int(get_val(row, "TO"))
                ast_to = round(assists / turnovers, 2) if turnovers else 0

                record_id = f"{game_id}_{player_name.replace(' ', '_')}"

                players.append({
                    "name": player_name,
                    "number": re.sub(r"\D", "", jersey),
                    "minutes_played": row.get("Min", ""),
                    "field_goals_made": fg_makes,
                    "field_goals_attempted": fg_attempts,
                    "field_goal_percent": float(get_val(row, "%", fallback=0)),
                    "two_pt_made": two_makes,
                    "two_pt_attempted": two_atts,
                    "two_pt_percent": float(get_val(row, "2 Points %", fallback=0)),
                    "three_pt_made": three_makes,
                    "three_pt_attempted": three_atts,
                    "three_pt_percent": float(get_val(row, "3 Points %", fallback=0)),
                    "free_throws_made": ft_makes,
                    "free_throws_attempted": ft_atts,
                    "free_throw_percent": float(get_val(row, "Free Throws %", fallback=0)),
                    "rebounds_o": parse_int(get_val(row, "Rebounds OR", "OR")),
                    "rebounds_d": parse_int(get_val(row, "DR", "DR")),
                    "rebounds_total": parse_int(get_val(row, "TOT", "TOT")),
                    "assists": assists,
                    "turnovers": turnovers,
                    "assist_turnover_ratio": ast_to,
                    "steals": parse_int(get_val(row, "ST")),
                    "blocks": parse_int(get_val(row, "BS")),
                    "personal_fouls": parse_int(get_val(row, "Fouls PF", "PF")),
                    "fouls_drawn": parse_int(get_val(row, "FD")),
                    "plus_minus": parse_int(get_val(row, "+/-")),
                    "points": pts,
                    "true_shooting_percent": round(ts_pct, 2),
                    "effective_fg_percent": round(efg, 2),
                    "game_id": game_id,
                    "game_date": game_date,
                    "team": team_name,
                    "opponent": opponent_name,
                    "home_team": home_team,
                    "away_team": away_team,
                    "record_id": record_id,
                    "league_id": league_id,
                    "created_at": datetime.utcnow().isoformat(),
                    "is_public": True
                })
            except Exception as err:
                print(f"⚠️ Skipping row due to: {err}")
                continue

    if not players:
        raise ValueError("❌ No valid player rows were parsed.")

    print(f"✅ Parsed {len(players)} players from {team} vs {opponent}")

    return {
        "players": players,
        "game": {
            "id": game_id,
            "game_date": game_date,
            "home_team": home_team,
            "away_team": away_team,
            "home_score": home_score,
            "away_score": away_score,
            "venue": venue,
            "created_at": datetime.utcnow().isoformat()
        }
    }
