import pdfplumber
import pandas as pd
import re
from datetime import datetime
from app.utils.helpers import get_val

def log(msg):
    import sys
    print(msg, file=sys.stdout, flush=True)

def log_to_file(msg):
    with open("parse_log.txt", "a") as f:
        f.write(f"[{datetime.utcnow().isoformat()}] {msg}\n")

def extract_player_stats(pdf_file, league_id):

    log_to_file("🛠 Starting PDF parse...")

    try:
        with pdfplumber.open(pdf_file) as pdf:
            if not pdf.pages:
                raise ValueError("❌ PDF has no pages.")

            first_page_text = pdf.pages[0].extract_text()
            log_to_file(f"📝 First page:\n{first_page_text}")

            match = re.search(r"^(.*)\s\d+\s[–-]\s\d+\s(.*)$", first_page_text, re.MULTILINE)
            team = match.group(1).strip() if match else "Unknown Team"
            opponent = match.group(2).strip() if match else "Unknown Opponent"

            date_match = re.search(r"(\d{1,2}\s+\w+\s+\d{4})", first_page_text)
            game_date = (
                datetime.strptime(date_match.group(1), "%d %b %Y").strftime("%Y-%m-%d")
                if date_match else "1970-01-01"
            )

            game_id = f"{game_date}_{team[:3].upper()}_vs_{opponent[:3].upper()}"

            all_tables = []
            for page in pdf.pages:
                tables = page.extract_tables()
                if tables:
                    all_tables.extend(tables)

            log_to_file(f"📑 Extracted {len(all_tables)} tables")
    except Exception as e:
        log_to_file(f"❌ Failed to open or read PDF: {e}")
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
        # Defensive check: skip short or malformed tables
        if len(stat_table) < 3:
            log_to_file("⚠️ Skipping table — not enough rows to build header + data")
            continue

        df = pd.DataFrame(stat_table)

        header1 = df.iloc[0].fillna("").astype(str)
        header2 = df.iloc[1].fillna("").astype(str)

        headers = []
        for h1, h2 in zip(header1, header2):
            combined = f"{h1} {h2}".strip()
            headers.append(combined or h1 or h2)

        log_to_file(f"🧩 Flattened headers:\n{headers}")

        if "Min" not in headers:
            log_to_file("⚠️ Skipping table — 'Min' column not found")
            continue

        df.columns = headers
        df = df[2:].reset_index(drop=True)
        df = df[df["Min"] != "DNP"]

        team_name = team if i == 0 else opponent
        opponent_name = opponent if i == 0 else team

        for _, row in df.iterrows():
            try:
                player_name = (row.get("name") or row.get("Name") or "").strip()
                jersey = str(row.get("No") or "")
                if not player_name or "Totals" in jersey or "Coach" in jersey:
                    continue

                def parse_made_attempts(val):
                    try: return map(int, val.split("/"))
                    except: return (0, 0)

                fg_makes, fg_attempts = parse_made_attempts(row.get("Field Goals M/A", "0/0"))
                ft_makes, ft_atts = parse_made_attempts(row.get("Free Throws M/A", "0/0"))
                two_makes, two_atts = parse_made_attempts(row.get("2 Points M/A", "0/0"))
                three_makes, three_atts = parse_made_attempts(row.get("3 Points M/A", "0/0"))

                pts = int(get_val(row, "PTS"))
                efg = ((fg_makes + 0.5 * three_makes) / fg_attempts) * 100 if fg_attempts else 0
                ts_denom = 2 * (fg_attempts + 0.44 * ft_atts)
                ts_pct = (pts / ts_denom) * 100 if ts_denom else 0
                assists = int(get_val(row, "AS"))
                turnovers = int(get_val(row, "TO"))
                ast_to = round(assists / turnovers, 2) if turnovers else 0

                record_id = f"{game_id}_{player_name.replace(' ', '_')}"

                # Fix ambiguous % headers
                for j, col in enumerate(headers):
                    if col == "%":
                        prev = headers[j - 1]
                        if "2 Points" in prev: headers[j] = "2 Points %"
                        elif "3 Points" in prev: headers[j] = "3 Points %"
                        elif "Free Throws" in prev: headers[j] = "Free Throws %"
                        else: headers[j] = f"{prev} %"

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
                    "rebounds_o": int(get_val(row, "Rebounds OR", "OR")),
                    "rebounds_d": int(get_val(row, "Rebounds DR", "DR")),
                    "rebounds_total": int(get_val(row, "Rebounds TOT", "TOT")),
                    "assists": assists,
                    "turnovers": turnovers,
                    "assist_turnover_ratio": ast_to,
                    "steals": int(get_val(row, "ST")),
                    "blocks": int(get_val(row, "BS")),
                    "personal_fouls": int(get_val(row, "Fouls PF", "PF")),
                    "fouls_drawn": int(get_val(row, "FD")),
                    "plus_minus": int(get_val(row, "+/-")),
                    "points": pts,
                    "true_shooting_percent": round(ts_pct, 2),
                    "effective_fg_percent": round(efg, 2),
                    "game_id": game_id,
                    "game_date": game_date,
                    "team": team_name,
                    "opponent": opponent_name,
                    "record_id": record_id,
                    "league_id": league_id,
                    "created_at": datetime.utcnow().isoformat(),
                    "is_public": True
                })
            except Exception as err:
                log_to_file(f"⚠️ Skipping row due to: {err}")
                continue

    if not players:
        raise ValueError("❌ No valid player rows were parsed.")

    log_to_file(f"✅ Parsed {len(players)} players from {team} vs {opponent}")
    return {"game_id": game_id, "team": team, "players": players}
