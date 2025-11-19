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

-   **PDF Parsing**: Utilizes PDFPlumber and PyMuPDF to extract game metadata and player statistics from various PDF box score formats using complex regex patterns.
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
-   **Advanced Team Analytics Engine**: The `app/utils/advanced_team_stats.py` module computes NBA-style advanced team metrics such as possession calculations, efficiency ratings, pace, shooting efficiency, rebounding percentages, Four Factors analysis, and PIE. These metrics are stored in `team_stats`.
    -   **Auto-Calculation on Upload**: Advanced team stats are automatically computed when Excel files are uploaded via `/api/parse`. The workflow: (1) `run_from_excel()` processes games and returns the league_id, (2) `fetch_team_stats_for_league()` retrieves all team records for that league, (3) `compute_team_advanced()` calculates and writes 37 advanced metrics back to Supabase. Comprehensive logging tracks league detection, processing progress, and warns about skipped games (e.g., missing opponent data). Failures in advanced stats computation are non-fatal and don't break Excel uploads.

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