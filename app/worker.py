#!/usr/bin/env python3
"""
FIBA LiveStats Game Poller Worker

This background worker polls games from game_schedule and triggers JSON parsing
when a game is detected as final.

How to run locally:
    python -m app.worker

Environment variables required:
    SUPABASE_URL - Your Supabase project URL
    SUPABASE_KEY - Supabase service role key (for production) or anon key (for dev)

Render worker start command:
    python -m app.worker

Polling cadence:
    - scheduled: every 2 minutes (or 15 minutes if matchtime is far away)
    - live: every 15 seconds
    - error: every 5 minutes
    - final: stops polling (next_poll_at = NULL)
"""

import os
import sys
import json
import time
import signal
import requests
from datetime import datetime, timedelta, timezone
from time import perf_counter

from supabase import create_client
from supabase.lib.client_options import ClientOptions

from app.utils.json_parser import parse_and_store_game


SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY")

PROBE_URL = os.environ.get("PROBE_LIVESTATS_URL")

if PROBE_URL:
    print("🔬 PROBE MODE — will fetch once and exit, no DB required")
elif not SUPABASE_URL or not SUPABASE_KEY:
    raise RuntimeError("Missing SUPABASE_URL or SUPABASE_KEY environment variables")

DB_SCHEMA = os.environ.get("DB_SCHEMA", "public")

game_db = None
if not PROBE_URL:
    game_db = create_client(SUPABASE_URL, SUPABASE_KEY, options=ClientOptions(schema=DB_SCHEMA))

ENABLE_LIVE_SYNC = os.environ.get("ENABLE_LIVE_SYNC", "true").lower() == "true"
LIVE_SYNC_SECONDS = int(os.environ.get("LIVE_SYNC_SECONDS", "10"))

print(f"DB_SCHEMA={DB_SCHEMA}")
print(f"game schema client={DB_SCHEMA}")
print(f"ENABLE_LIVE_SYNC={ENABLE_LIVE_SYNC}, LIVE_SYNC_SECONDS={LIVE_SYNC_SECONDS}")

POLL_INTERVAL = 10
LIVESTATS_BASE = "https://fibalivestats.dcd.shared.geniussports.com/data"

_shutdown_requested = False

def _handle_sigterm(signum, frame):
    global _shutdown_requested
    print("\n🛑 SIGTERM received — finishing current poll then shutting down...")
    _shutdown_requested = True

signal.signal(signal.SIGTERM, _handle_sigterm)
signal.signal(signal.SIGINT, _handle_sigterm)

REQUEST_HEADERS = {
    "User-Agent": "SwishAssistant/1.0 (LiveStats Poller)",
    "Accept": "application/json",
    "Accept-Encoding": "gzip, deflate, br",
}

_etag_cache: dict[str, str] = {}
_last_modified_cache: dict[str, str] = {}


def get_due_games():
    """
    Fetch games that are due for polling.
    Uses multiple queries and merges by game_key.
    
    Query A1: status IN ('scheduled','live','error') AND matchtime in window AND next_poll_at <= now
    Query A2: status IN ('scheduled','live','error') AND matchtime in window AND next_poll_at IS NULL (never polled)
    Query B1: status='final' AND parsed_at IS NULL AND next_poll_at <= now
    Query B2: status='final' AND parsed_at IS NULL AND next_poll_at IS NULL (never polled)
    """
    now = datetime.now(timezone.utc)
    now_iso = now.isoformat()
    window_start = (now - timedelta(hours=12)).isoformat()
    window_end = (now + timedelta(hours=36)).isoformat()
    
    select_cols = 'game_key, competitionname, matchtime, hometeam, awayteam, "LiveStats URL", league_id, status, poll_fail_count, parsed_at, last_polled_at, poll_count, total_poll_bytes'
    
    games_by_key = {}
    
    try:
        result_a1 = (
            game_db.table("game_schedule")
            .select(select_cols)
            .in_("status", ["scheduled", "live", "error"])
            .gte("matchtime", window_start)
            .lte("matchtime", window_end)
            .lte("next_poll_at", now_iso)
            .execute()
        )
        for g in result_a1.data or []:
            games_by_key[g["game_key"]] = g
    except Exception as e:
        print(f"⚠️ Query A1 failed: {e}")
    
    try:
        result_a2 = (
            game_db.table("game_schedule")
            .select(select_cols)
            .in_("status", ["scheduled", "live", "error"])
            .gte("matchtime", window_start)
            .lte("matchtime", window_end)
            .is_("next_poll_at", "null")
            .execute()
        )
        for g in result_a2.data or []:
            games_by_key[g["game_key"]] = g
    except Exception as e:
        print(f"⚠️ Query A2 failed: {e}")
    
    try:
        result_b1 = (
            game_db.table("game_schedule")
            .select(select_cols)
            .eq("status", "final")
            .is_("parsed_at", "null")
            .lte("next_poll_at", now_iso)
            .execute()
        )
        for g in result_b1.data or []:
            games_by_key[g["game_key"]] = g
    except Exception as e:
        print(f"⚠️ Query B1 failed: {e}")
    
    try:
        result_b2 = (
            game_db.table("game_schedule")
            .select(select_cols)
            .eq("status", "final")
            .is_("parsed_at", "null")
            .is_("next_poll_at", "null")
            .execute()
        )
        for g in result_b2.data or []:
            games_by_key[g["game_key"]] = g
    except Exception as e:
        print(f"⚠️ Query B2 failed: {e}")
    
    return list(games_by_key.values())


def extract_numeric_id(livestats_url: str | None) -> str | None:
    """Extract the numeric ID from the LiveStats URL (last path segment)."""
    if not livestats_url:
        return None
    return livestats_url.rstrip("/").split("/")[-1]


def detect_game_status(data: dict, current_status: str) -> str:
    """
    Detect game status from JSON data.
    
    Returns: 'final', 'live', or 'scheduled'
    """
    if data.get("matchStatus") in ("FINISHED", "FINAL", "COMPLETED"):
        return "final"
    
    if data.get("gameEnded") is True:
        return "final"
    
    clock = data.get("clock", {})
    if isinstance(clock, dict):
        if clock.get("gameEnded") is True or clock.get("periodType") == "FINISHED":
            return "final"
        
        clock_time = clock.get("time", "")
        clock_running = clock.get("running", True)
        period = data.get("period", 0)
        max_periods = data.get("periodsMax", 4)
        if not clock_running and period >= max_periods:
            if clock_time in ("00:00:00", "0:00:00", "00:00", "0:00", "PT0S"):
                return "final"
    
    period_info = data.get("period")
    if period_info and isinstance(period_info, int):
        max_periods = data.get("periodsMax", 4)
        if period_info >= max_periods:
            period_type = data.get("periodType", "")
            if period_type in ("FINISHED", "FINAL"):
                return "final"
    
    pbp = data.get("pbp", [])
    if pbp:
        sorted_pbp = sorted(
            [e for e in pbp if e.get("actionNumber") is not None],
            key=lambda e: e.get("actionNumber", 0),
            reverse=True
        )
        for event in sorted_pbp[:10]:
            action_type = event.get("actionType", "")
            if action_type in ("game", "endgame"):
                sub_type = event.get("subType", "")
                if sub_type in ("end", "final", "finished"):
                    return "final"
    
    if pbp:
        return "live"
    
    return current_status if current_status in ("scheduled", "error") else "scheduled"


def compute_next_poll(status: str, matchtime_str: str | None) -> str | None:
    """
    Compute next_poll_at based on status and cadence.
    
    Returns ISO timestamp string or None (to stop polling).
    """
    now = datetime.now(timezone.utc)
    
    if status == "final":
        return None
    
    if status == "live":
        return (now + timedelta(seconds=15)).isoformat()
    
    if status == "error":
        return (now + timedelta(minutes=5)).isoformat()
    
    if matchtime_str:
        try:
            if matchtime_str.endswith("Z"):
                matchtime = datetime.fromisoformat(matchtime_str.replace("Z", "+00:00"))
            else:
                matchtime = datetime.fromisoformat(matchtime_str)
            
            if matchtime.tzinfo is None:
                matchtime = matchtime.replace(tzinfo=timezone.utc)
            
            time_until_game = (matchtime - now).total_seconds()
            
            if time_until_game > 3600:
                return (now + timedelta(minutes=15)).isoformat()
            elif time_until_game > 600:
                return (now + timedelta(minutes=2)).isoformat()
        except Exception:
            pass
    
    return (now + timedelta(seconds=120)).isoformat()


def is_live_sync_due(last_polled_at: str | None) -> bool:
    """Check if live sync is due based on last_polled_at timestamp."""
    if not ENABLE_LIVE_SYNC:
        return False
    if last_polled_at is None:
        return True
    try:
        if last_polled_at.endswith("Z"):
            last_sync = datetime.fromisoformat(last_polled_at.replace("Z", "+00:00"))
        else:
            last_sync = datetime.fromisoformat(last_polled_at)
        if last_sync.tzinfo is None:
            last_sync = last_sync.replace(tzinfo=timezone.utc)
        elapsed = (datetime.now(timezone.utc) - last_sync).total_seconds()
        return elapsed >= LIVE_SYNC_SECONDS
    except Exception:
        return True


def fetch_livestats_json(data_url: str, game_key: str = "probe"):
    """
    Fetch LiveStats JSON with instrumentation.
    Returns (response, data, metrics) or raises on failure.
    metrics dict has: bytes_in, download_ms, content_encoding, content_length,
                      etag, last_modified, cache_hit, pbp_total
    """
    headers = dict(REQUEST_HEADERS)

    cached_etag = _etag_cache.get(game_key)
    cached_lm = _last_modified_cache.get(game_key)
    if cached_etag:
        headers["If-None-Match"] = cached_etag
    if cached_lm:
        headers["If-Modified-Since"] = cached_lm

    t0 = perf_counter()
    response = requests.get(data_url, timeout=15, headers=headers)
    download_ms = round((perf_counter() - t0) * 1000, 1)

    content_encoding = response.headers.get("Content-Encoding", "none")
    content_length = response.headers.get("Content-Length", "n/a")
    etag = response.headers.get("ETag")
    last_modified = response.headers.get("Last-Modified")

    if etag:
        _etag_cache[game_key] = etag
    if last_modified:
        _last_modified_cache[game_key] = last_modified

    metrics = {
        "status_code": response.status_code,
        "bytes_in": len(response.content),
        "download_ms": download_ms,
        "content_encoding": content_encoding,
        "content_length": content_length,
        "etag": etag or "none",
        "last_modified": last_modified or "none",
        "cache_hit": response.status_code == 304,
    }

    if response.status_code == 304:
        return response, None, metrics

    data = response.json() if response.status_code == 200 else None
    if data:
        metrics["pbp_total"] = len(data.get("pbp", []))
    return response, data, metrics


def log_poll_metrics(game_key: str, game_status: str, poll_number: int, metrics: dict, parse_info: dict | None = None):
    """Print a single structured log line with all poll metrics."""
    parts = [
        f"game_key={game_key}",
        f"status={game_status}",
        f"poll={poll_number}",
        f"http={metrics['status_code']}",
        f"bytes_in={metrics['bytes_in']:,}",
        f"dl_ms={metrics['download_ms']}",
        f"encoding={metrics['content_encoding']}",
        f"content_length={metrics['content_length']}",
        f"etag={metrics['etag']}",
        f"last_modified={metrics['last_modified']}",
        f"cache_hit={metrics['cache_hit']}",
    ]
    if parse_info:
        parts.extend([
            f"parse_ms={parse_info.get('parse_ms', 'n/a')}",
            f"pbp_total={parse_info.get('pbp_total', 'n/a')}",
        ])
    print(f"   📈 POLL_LOG | {' | '.join(parts)}")


def poll_game(game: dict):
    """
    Poll a single game: fetch JSON, detect status, update DB, parse if needed.
    
    Parsing triggers:
    - FINAL: parse once if parsed_at is null, then set parsed_at
    - LIVE: parse every LIVE_SYNC_SECONDS if ENABLE_LIVE_SYNC, based on last_polled_at
    """
    game_key = game["game_key"]
    livestats_url = game.get("LiveStats URL")
    current_status = game.get("status", "scheduled")
    poll_fail_count = game.get("poll_fail_count") or 0
    matchtime = game.get("matchtime")
    last_polled_at = game.get("last_polled_at")
    prev_poll_count = game.get("poll_count") or 0
    prev_total_bytes = game.get("total_poll_bytes") or 0
    
    print(f"\n🎯 Polling: {game_key} (status: {current_status})")
    
    numeric_id = extract_numeric_id(livestats_url)
    if not numeric_id:
        print(f"   ⚠️ No numeric ID found in URL: {livestats_url}")
        return
    
    data_url = f"{LIVESTATS_BASE}/{numeric_id}/data.json"
    
    now_iso = datetime.now(timezone.utc).isoformat()
    
    try:
        response, data, metrics = fetch_livestats_json(data_url, game_key)
        response_bytes = metrics["bytes_in"]
        new_poll_count = prev_poll_count + 1

        if metrics["cache_hit"]:
            log_poll_metrics(game_key, current_status, new_poll_count, metrics)
            print(f"   ⚡ 304 Not Modified — skipping parse")
            game_db.table("game_schedule").update({
                "last_polled_at": now_iso,
                "poll_count": new_poll_count,
                "poll_bytes_recent": 0,
                "next_poll_at": compute_next_poll(current_status, matchtime),
            }).eq("game_key", game_key).execute()
            return
        
        if response.status_code != 200:
            if response.status_code in (403, 404):
                print(f"   ⏭️ HTTP {response.status_code} - game not ready yet (scheduled)")
                update_data = {
                    "last_polled_at": now_iso,
                    "next_poll_at": compute_next_poll("scheduled", matchtime),
                }
            else:
                print(f"   ⚠️ HTTP {response.status_code} - unexpected error")
                update_data = {
                    "last_polled_at": now_iso,
                    "poll_fail_count": poll_fail_count + 1,
                    "next_poll_at": compute_next_poll(current_status, matchtime),
                }
            game_db.table("game_schedule").update(update_data).eq("game_key", game_key).execute()
            return
        
    except Exception as e:
        print(f"   ❌ Request failed: {e}")
        update_data = {
            "last_polled_at": now_iso,
            "poll_fail_count": poll_fail_count + 1,
            "status": "error" if poll_fail_count >= 2 else current_status,
            "next_poll_at": compute_next_poll("error" if poll_fail_count >= 2 else current_status, matchtime),
        }
        game_db.table("game_schedule").update(update_data).eq("game_key", game_key).execute()
        return
    
    new_status = detect_game_status(data, current_status)
    new_total_bytes = prev_total_bytes + response_bytes

    log_poll_metrics(game_key, new_status, new_poll_count, metrics)
    
    update_data = {
        "status": new_status,
        "last_polled_at": now_iso,
        "poll_fail_count": 0,
        "next_poll_at": compute_next_poll(new_status, matchtime),
        "poll_count": new_poll_count,
        "poll_bytes_recent": response_bytes,
        "total_poll_bytes": new_total_bytes,
    }
    
    if new_status == "final" and current_status != "final":
        update_data["final_detected_at"] = now_iso
    
    game_db.table("game_schedule").update(update_data).eq("game_key", game_key).execute()
    
    should_parse = False
    parse_reason = None
    
    if new_status == "final" and game.get("parsed_at") is None:
        should_parse = True
        parse_reason = "final"
    elif new_status == "live" and is_live_sync_due(last_polled_at):
        should_parse = True
        parse_reason = "live_sync"
    
    if should_parse:
        if parse_reason == "final":
            print(f"   🔄 Parsing final game...")
        else:
            print(f"   🔄 Live sync: parsing game {game_key}")
        
        try:
            t_parse = perf_counter()
            parse_and_store_game(
                numeric_id=numeric_id,
                league_name=game.get("competitionname", "Unknown League"),
                game_date=matchtime,
                home_team_name=game.get("hometeam"),
                away_team_name=game.get("awayteam"),
                game_key=game_key,
                livestats_url=livestats_url,
            )
            parse_ms = round((perf_counter() - t_parse) * 1000, 1)
            print(f"   ⏱️ Parse completed in {parse_ms}ms | pbp_total={metrics.get('pbp_total', 'n/a')}")
            
            if parse_reason == "final":
                game_db.table("game_schedule").update({
                    "parsed_at": now_iso,
                }).eq("game_key", game_key).execute()
                print(f"   ✅ Final parse complete!")
            else:
                print(f"   ✅ Live sync complete!")
            
        except Exception as e:
            print(f"   ❌ Parse failed: {e}")
            game_db.table("game_schedule").update({
                "status": "error",
                "next_poll_at": (datetime.now(timezone.utc) + timedelta(minutes=5)).isoformat(),
            }).eq("game_key", game_key).execute()


def run_worker():
    """
    Main worker loop - runs continuously polling games.
    """
    print("=" * 60)
    print("🏀 FIBA LiveStats Game Poller Worker Started")
    print("=" * 60)
    print(f"Poll interval: {POLL_INTERVAL}s")
    print(f"Supabase URL: {SUPABASE_URL[:30]}...")
    print("=" * 60)
    
    while not _shutdown_requested:
        try:
            games = get_due_games()
            
            if games:
                print(f"\n📋 Found {len(games)} games due for polling")
                for game in games:
                    if _shutdown_requested:
                        print("🛑 Shutdown requested — stopping mid-batch")
                        break
                    try:
                        poll_game(game)
                    except Exception as e:
                        print(f"   ❌ Error polling {game.get('game_key')}: {e}")
            else:
                print(".", end="", flush=True)
            
        except Exception as e:
            print(f"\n❌ Worker loop error: {e}")
        
        for _ in range(POLL_INTERVAL * 10):
            if _shutdown_requested:
                break
            time.sleep(0.1)

    print("👋 Worker shut down cleanly.")


def run_probe():
    """
    Probe mode: fetch a single LiveStats URL once, print all metrics, then exit.
    Set PROBE_LIVESTATS_URL env var to the full data.json URL or the webcast page URL.
    """
    url = PROBE_URL.strip()
    if "/data.json" not in url:
        numeric_id = url.rstrip("/").split("/")[-1]
        url = f"{LIVESTATS_BASE}/{numeric_id}/data.json"

    probe_key = os.environ.get("PROBE_GAME_KEY", "probe")

    print(f"\n{'='*60}")
    print(f"🔬 PROBE: {url}")
    print(f"{'='*60}")

    try:
        response, data, metrics = fetch_livestats_json(url, probe_key)
    except Exception as e:
        print(f"❌ Probe request failed: {e}")
        sys.exit(1)

    print(f"\n📡 Response Headers:")
    print(f"   HTTP Status:       {metrics['status_code']}")
    print(f"   Content-Encoding:  {metrics['content_encoding']}")
    print(f"   Content-Length:     {metrics['content_length']}")
    print(f"   ETag:              {metrics['etag']}")
    print(f"   Last-Modified:     {metrics['last_modified']}")

    print(f"\n📦 Transfer:")
    print(f"   Bytes downloaded:  {metrics['bytes_in']:,}")
    print(f"   Download time:     {metrics['download_ms']}ms")

    if data:
        pbp = data.get("pbp", [])
        teams = data.get("tm", {})
        print(f"\n🏀 Game Data:")
        print(f"   PBP events:        {len(pbp)}")
        if pbp:
            action_numbers = [e.get("actionNumber", 0) for e in pbp if e.get("actionNumber") is not None]
            if action_numbers:
                print(f"   Action# range:     {min(action_numbers)} - {max(action_numbers)}")
        for tid, team in teams.items():
            team_name = team.get("nameTeam", team.get("name", tid))
            players = team.get("pl", {})
            print(f"   Team: {team_name} ({len(players)} players)")

        status = detect_game_status(data, "scheduled")
        print(f"   Detected status:   {status}")

        json_size = len(json.dumps(data))
        print(f"   JSON size (raw):   {json_size:,} bytes")
    else:
        print(f"\n⚠️ No data returned (HTTP {metrics['status_code']})")

    print(f"\n{'='*60}")
    print(f"🔬 Probe complete. Run again with same URL to test caching:")
    print(f"   If ETag/Last-Modified was returned, the second request will")
    print(f"   send If-None-Match/If-Modified-Since and may get a 304.")
    print(f"{'='*60}")


if __name__ == "__main__":
    if PROBE_URL:
        run_probe()
    else:
        run_worker()
