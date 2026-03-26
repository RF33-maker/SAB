# Basketball Stats Analysis Platform

## Overview
This project is a Flask-based basketball statistics analysis platform. Its main purpose is to process game data from various sources (PDF box scores, Excel files, live data feeds), store player and team statistics in Supabase, and provide AI-powered insights using OpenAI's Assistant API. The platform aims to offer comprehensive analysis, visualization, and querying capabilities for basketball statistics, supporting both historical data analysis and real-time game updates.

## User Preferences
Preferred communication style: Simple, everyday language.

## System Architecture

### Backend Framework
The platform uses Flask with Python 3.x, leveraging a lightweight and flexible framework for building REST APIs. It employs blueprint-based routing for modularity (`parse_bp`, `query_bp`, `chart_bp`) and separates business logic into utility modules. Asynchronous support is integrated via `asyncio`.

### API Structure
Key API endpoints include `/api/parse` for file uploads, `/start` and `/reset` for OpenAI conversation management, `/players` for listing players, and `/chart_summary/<player_name>` for generating statistical summaries. Rate limiting (100 requests/day, 10 requests/minute) and a permissive CORS policy are implemented.

### Data Processing Pipeline

-   **PDF Parsing (Genius Sports post-game PDFs)**: A dedicated ingestion pipeline in `app/utils/pdf_parser.py` handles Genius Sports / FIBA post-game PDF exports produced at Richards Elite and similar events where live JSON stats are unavailable. The pipeline auto-detects report type from the first-page header and routes to sub-parsers:
    -   **box_score**: Extracts per-player stats (24-column regex), team totals, and footer stats (paint points, bench points, turnovers, fast-break, etc.) → writes to `player_stats` and `team_stats` (test schema).
    -   **pbp** (play-by-play): Uses layout-based column detection (home ~col 12, away ~col 48) to reconstruct play-by-play events with side attribution → writes to `live_events` (test schema).
    -   **lineup**: Parses Lineup Analysis tables with multi-line lineup text and per-row stat columns → writes to `lineup_stats` (test schema). Team section detection anchored to metadata-derived team names via `_is_known_team_header`.
    -   **plus_minus**: Extracts per-player plus/minus summary (mins on/off, pts diff on/off) → writes to `player_plus_minus` (test schema). `identifier_duplicate` keyed on `game_key + player_id`. Team detection anchored to metadata.
    -   **rotations**: Parses multi-line Rotations Summary blocks (lineup header + stat row) → writes to `rotations_summary` (test schema). Team detection anchored to metadata.
    -   **shot_chart / shot_areas**: Correctly detected and skipped (image-only PDFs, no extractable data).
    -   Team linkage: `_resolve_team_from_meta(meta, league_id)` pre-resolves home/away IDs from PDF header; `_is_known_team_header(line, known_names)` matches section headers exactly — no fragile text heuristics.
    -   game_key defaults to `PDF_{game_no}` from the PDF header. All writes target `test` schema for safety.
    -   Exposed via the `/api/parse-pdf` endpoint (POST, multipart/form-data: `file`, `league_name`, optional `game_key`/`user_id`).
-   **Legacy parser.py**: Marked deprecated; all PDF ingestion now uses `pdf_parser.py`.
-   **PDF schema additions** (`migrations/pdf_tables.sql`): 15 new `team_stats` columns (paint pts, bench pts, turnovers, fast-break, etc.), attendance/officials on `game_schedule`, and 3 new tables: `lineup_stats`, `player_plus_minus`, `rotations_summary`.
-   **JSON Parser fixes** (`json_parser.py`): Fixed 4 wrong TEAM_FIELD_MAP keys (`tot_sTimeLeading`, `tot_sBiggestScoringRun`, `tot_sLeadChanges`, `tot_sTimesScoresLevel`), added 1 missing key (`tot_sBiggestLead`), added 7 unmapped fields, updated `lds`→`game_leaders_json`, `source_type` tag, and attendance/officials upsert from JSON.
-   **Legacy PDF Parsing**: Utilizes PDFPlumber and PyMuPDF to extract game metadata and player statistics from various PDF box score formats using complex regex patterns.
-   **Excel Parsing (Bulk Import)**: Employs Pandas for structured data processing, featuring a field mapping system for standardization and optional "Pool" column support. It prioritizes schedule-first processing, adding games to `game_schedule` before fetching LiveStats. Smart change detection skips unchanged games on repeat uploads, significantly reducing database calls.
-   **Live Game Parser (Real-time)**: A continuous polling system (`app/live_parser.py`) processes live games from `game_schedule` data.json URLs. It extracts comprehensive game data (team stats, player stats, plays, shots), normalizes player/team names, and syncs data to `team_stats`, `player_stats`, `live_events`, and `shot_chart`. A status field system (`live` vs. `final`) manages data integrity and optimizes performance by excluding finalized games from polling.
-   **Play-by-Play Backfill System**: Backfills historical play-by-play data from FIBA LiveStats JSON into the `live_events` table, including an optimized fuzzy player matching system and team name normalization.
-   **Deduplication System**: Handles team name variations and gender markers through alias mapping and strips gender indicators. Player fuzzy matching (85% similarity threshold) prevents duplicate player records within the same team. Maintenance scripts (`cleanup_duplicate_teams.py`, `cleanup_duplicate_players.py`) are available for merging existing duplicates.

### Data Storage
Supabase (PostgreSQL-based) is used for data storage. The schema includes a `player_stats` table for individual game performance, denormalized for query performance, and supports league ID isolation.

### AI Integration (RAG Architecture)
The platform integrates OpenAI's Assistant API with a Retrieval-Augmented Generation (RAG) architecture. This involves:
1.  **Entity Detection**: Identifies player names, team names, or league context in user questions using database lookups.
2.  **Context Builders**: Functions like `build_player_context()`, `build_team_context()`, `build_league_context()`, and `build_general_context()` fetch relevant data from Supabase tables (`player_stats`, `teams`, `players`, `game_schedule`, `team_stats`, `leagues`) and structure it as JSON for the AI.
The AI is instructed to use *only* the provided context, respond with exact numbers, and acknowledge missing data, ensuring factual and hallucination-free responses.

### Analytics & Visualization
-   **Chart Data Generation**: Calculates key statistics and compares game performance against season averages.
-   **Game Summaries**: AI-generated summaries using "Four Factors" basketball analytics.
-   **Advanced Team Analytics Engine**: The `app/utils/advanced_team_stats.py` module computes NBA-style advanced team metrics such as possession calculations, efficiency ratings, pace, shooting efficiency, rebounding percentages, Four Factors analysis, and PIE. These metrics are stored in `team_stats` with 37 advanced stat columns.
-   **Advanced Player Analytics Engine**: The `app/utils/advanced_player_stats.py` module computes NBA-style advanced player metrics including Usage%, eFG%, True Shooting%, assist percentage, rebounding percentages, turnover percentage, PIE (Player Impact Estimate), estimated offensive/defensive ratings, and scoring distribution breakdowns. These metrics are stored in `player_stats` with 22 advanced stat columns. The engine requires a `team_map` data structure (mapping game_key → team_id → team_stats) to provide team and opponent context for player calculations. Minutes are converted from "MM:SS" format to decimal for accurate usage rate calculations.
-   **Advanced Stats Coordinator**: The `app/utils/compute_advanced_stats.py` module orchestrates the complete advanced stats pipeline, ensuring correct execution order and data validation:
    1. Computes team advanced stats first (to generate possessions data)
    2. Re-fetches updated team stats with calculated possessions
    3. Validates games have exactly 2 teams and both have possessions
    4. Builds team_map context for player calculations
    5. Computes player advanced stats using team context
    6. Returns detailed status with processed/skipped/failure counts
    -   **Auto-Calculation on Excel Upload**: When Excel files are uploaded via `/api/parse`, the entire advanced stats pipeline runs automatically after all games are processed. The workflow: (1) `run_from_excel()` processes all games and captures league_id, (2) `compute_advanced_stats(league_id)` runs the complete team + player analytics pipeline, (3) All 59 advanced metrics (37 team + 22 player) are written to Supabase. Comprehensive logging tracks each step, validates data integrity, and reports processing results. Failures in advanced stats computation are non-fatal and don't break Excel uploads.

## External Dependencies

### Third-Party Services
-   **Supabase**: Provides PostgreSQL database and file storage. Requires `SUPABASE_URL` and `SUPABASE_KEY`.
-   **OpenAI API**: Used for AI-powered analysis and natural language queries via the Assistant API. Requires `OPENAI_API_KEY`.

### Python Packages
-   **Core Framework**: `Flask`, `flask_cors`, `Flask-Limiter`, `gunicorn`.
-   **Data Processing**: `pandas`, `pdfplumber`, `PyMuPDF`.
-   **External Integrations**: `openai`, `supabase`, `requests`.
-   **Utilities**: `asyncio`.

### Environment Configuration
The application requires the following environment variables:
-   `OPENAI_API_KEY`
-   `SUPABASE_URL`
-   `SUPABASE_KEY`

### File Storage
Uploaded Excel and PDF files are stored in Supabase Storage buckets under `user-uploads/`, with a path structure of `{user_id}/{filename}`.