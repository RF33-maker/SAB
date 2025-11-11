# Basketball Stats Analysis Platform

## Overview

This is a Flask-based basketball statistics analysis platform that processes game data from PDF box scores and Excel files, stores player statistics in Supabase, and provides AI-powered insights through OpenAI's Assistant API. The system enables users to upload game data, query player performance, analyze trends, and visualize statistics through a REST API.

## User Preferences

Preferred communication style: Simple, everyday language.

## System Architecture

### Backend Framework
**Technology**: Flask with Python 3.x

**Rationale**: Flask provides a lightweight, flexible framework suitable for building REST APIs. The modular blueprint structure allows for clean separation of concerns across parsing, querying, and charting functionalities.

**Key Design Patterns**:
- Blueprint-based routing for modular endpoint organization (`parse_bp`, `query_bp`, `chart_bp`)
- Separation of business logic into utility modules
- Asynchronous support through `asyncio` for handling concurrent operations

### API Structure

**Endpoints**:
- `/api/parse` - Handles file upload and parsing (PDF/Excel)
- `/start` - Initializes OpenAI conversation threads
- `/reset` - Creates new conversation contexts
- `/players` - Lists all players from database
- `/chart_summary/<player_name>` - Generates statistical summaries for visualization

**Rate Limiting**: Flask-Limiter with memory-based storage (100 requests/day, 10 requests/minute defaults)

**CORS Policy**: Permissive configuration allowing all origins with credential support

### Data Processing Pipeline

**PDF Parsing**:
- **Technology**: PDFPlumber for text extraction, PyMuPDF as fallback
- **Process**: Extracts game metadata (date, teams, venue, scores) and individual player statistics from box score PDFs
- **Challenge Addressed**: Complex regex patterns handle various box score formats and team name variations

**Excel Parsing** (Administrative/Bulk Import Tool):
- **Technology**: Pandas for structured data processing
- **Process**: Field mapping system translates various column naming conventions to standardized schema
- **Normalization**: Handles case-insensitive field matching and multiple naming variants
- **Pool Support**: Optional "Pool" column for leagues with multiple pools (e.g., NBL Division 1). Automatically detected and stored in game_schedule when present; gracefully skipped for leagues without pools
- **Schedule-First Processing**: Games are added to game_schedule immediately from Excel data, then LiveStats data is fetched if available. This enables future/unplayed games to appear in schedules while stats are processed separately for completed games

**Live Game Parser** (Primary Production Parser - Added October 2025):
- **Technology**: Continuous polling system (`app/live_parser.py`)
- **Purpose**: Real-time processing of live games as primary data source
- **Process**:
  - Polls `game_schedule` table for games with LiveStats data.json URLs
  - Extracts complete game data: team stats, player stats, plays, shots
  - Uses league_id context from game_schedule for proper entity relationships
  - Auto-creates teams/players if not pre-loaded from Excel
  - Syncs data to: `team_stats`, `player_stats`, `live_events`, `shot_chart`
- **Data Integrity**:
  - Normalizes all player/team names using shared functions from json_parser
  - Composite conflict keys prevent cross-game data corruption
  - Player ID mapping keyed by (team_id, name) to handle duplicate names across teams
  - Proper foreign key relationships: game_key, league_id, team_id, player_id
- **Status Field System** (Best Practice for Live vs Final Stats):
  - All stats initially marked with `status: 'live'` during active games
  - Stats continuously update via upsert as game progresses
  - Automatic game completion detection checks: explicit status flags, period >= 4 with expired clock, end-of-game play markers
  - When game finishes, all records automatically updated to `status: 'final'`
  - Finalized games excluded from polling to optimize performance
  - Query design: Use `WHERE status = 'final'` for season stats/historical analysis, `WHERE status = 'live'` for real-time box scores
- **Production Design**: 10-second polling interval, graceful error handling, traceback logging

**Data Validation**: Player name normalization removes captain designations (C) and handles bracket variations for consistent database queries

**Play-by-Play Backfill System** (Added November 2025):
- **Purpose**: Backfill historical play-by-play data from FIBA LiveStats JSON into `live_events` table
- **Scripts**:
  - `app/backfill_pbp_optimized.py` - Main backfill script with fuzzy player matching
  - `app/fix_missing_ids_simple.py` - Optimized upsert script to fill missing player_id/team_id gaps
- **Current Status** (as of November 11, 2025):
  - **98,718 events** inserted from ~197 completed games
  - **0 duplicate players** created (maintained baseline of 1,123 players)
  - **61.7% player match rate** (60,937 events with player_id)
  - **38.3% unmatched** (37,781 events) - primarily due to player name variations and data quality issues
- **Data Quality**:
  - Events with NULL player_name (administrative events like "start of period", "end of game") correctly have NULL team_id/player_id
  - Events with player_name should have both team_id and player_id for accurate stats
  - Unmatched records (38.3%) have player names that don't match database records even with fuzzy matching (0.75 threshold)
- **Fix Process** (Completed):
  - Fixed team name normalization to handle abbreviations (e.g., "MK Breakers" → "Milton Keynes Breakers")
  - Script loads all teams and players into memory for fast fuzzy matching
  - Successfully filled 6,637 missing IDs, improving match rate from 55% to 61.7%
  - Remaining unmatched records require manual data cleanup or lower matching thresholds
- **Design Decisions**:
  - Team name normalization with aliases handles common abbreviations
  - In-memory player loading eliminates thousands of API calls for dramatic performance improvement
  - Fuzzy matching (0.75 threshold) balances accuracy with match rate
  - Batch processing with retry logic handles connection errors gracefully
  - Upsert approach allows resuming/fixing without reprocessing entire dataset

**Deduplication System** (Added October 2025):
- **Team Normalization**: Handles team name variations and gender markers
  - Alias mapping (e.g., "MK Breakers" → "Milton Keynes Breakers")
  - Strips gender indicators: (M), (W), (Men), (Women), (Male), (Female)
  - Applied transparently at lookup layer without changing parsers
- **Player Fuzzy Matching**: Prevents duplicate player records
  - Uses team_id constraint to safely match similar names within same team
  - 85% similarity threshold using Python's difflib.SequenceMatcher
  - Matches "W White" with "Will White", handles typos and abbreviations
- **Cleanup Scripts**: Two maintenance scripts for merging existing duplicates
  - `cleanup_duplicate_teams.py` - Consolidates team variations
  - `cleanup_duplicate_players.py` - Merges player name variations
  - Both update canonical records to normalized names and rewrite all foreign key references

### Data Storage

**Database**: Supabase (PostgreSQL-based)

**Schema Design**:
- `player_stats` table stores individual game performance records
- Key fields: player name, game date, team, opponent, comprehensive statistics (points, rebounds, assists, shooting percentages, etc.)
- League ID support for multi-league data isolation

**Rationale**: Supabase provides a managed PostgreSQL database with built-in REST API, authentication, and real-time capabilities. The schema is denormalized for query performance, storing all stats in a single table rather than normalized relations.

### AI Integration (RAG Architecture)

**OpenAI Assistant API with Retrieval-Augmented Generation**:
- Persistent assistant optimized for context-based responses
- Thread-based conversation management for context retention
- RAG flow: Pre-fetch relevant data → Build context → Send to AI agent

**RAG Pipeline** (Added October 2025):
1. **Entity Detection** (`app/utils/rag_utils.py`):
   - Analyzes user questions to identify player names, team names, or league context
   - Uses database lookups and keyword matching for precision
   - Supports league_id filtering for accurate entity resolution

2. **Context Builders**:
   - `build_player_context()` - Fetches recent games, season averages, team info from Supabase
   - `build_team_context()` - Retrieves team info, roster, recent games, team stats
   - `build_league_context()` - Aggregates league info, teams, top scorers, upcoming games
   - `build_general_context()` - Handles open-ended queries with league-wide data

3. **Data Sources**:
   - Queries Supabase tables: `player_stats`, `player_season_averages`, `teams`, `players`, `game_schedule`, `team_stats`, `leagues`
   - Structures JSON context optimized for AI consumption
   - Filters data by league_id when provided for precise results

**Assistant Instructions**:
- Trained to use ONLY provided CONTEXT data (no hallucinations)
- Responds with exact numbers and stats from database
- Acknowledges when data is missing rather than guessing
- Natural, conversational responses grounded in factual data

**Design Decision**: RAG approach ensures 100% accuracy by pre-fetching all relevant data and explicitly providing it to the AI. Unlike function calling, the AI cannot request external data—it must work with the structured context provided, eliminating potential hallucinations.

### Analytics & Visualization

**Chart Data Generation**:
- Calculates key statistics: points, assists, rebounds, turnovers, steals, blocks, shooting percentages
- Compares last game vs. previous game vs. season average
- Statistical summaries use Python's `statistics` module for averages

**Game Summaries**:
- AI-generated using "Four Factors" basketball analytics framework
- Metrics: Effective Field Goal %, Turnover Rate, Rebounding Rate, Free Throw Rate
- Identifies top performers and key game insights

## External Dependencies

### Third-Party Services

**Supabase**:
- Purpose: Database and file storage
- Configuration: Requires `SUPABASE_URL` and `SUPABASE_KEY` environment variables
- Usage: Player stats storage, file uploads (user-uploads bucket)

**OpenAI API**:
- Purpose: AI-powered basketball analysis and natural language queries
- Configuration: Requires `OPENAI_API_KEY` environment variable
- Models: Uses Assistant API with function calling for structured data retrieval

### Python Packages

**Core Framework**:
- `Flask==3.1.1` - Web framework
- `flask_cors==5.0.1` - CORS handling
- `Flask-Limiter==3.12` - Rate limiting
- `gunicorn==22.0.0` - Production WSGI server

**Data Processing**:
- `pandas==2.3.1` - Excel/CSV data manipulation
- `pdfplumber==0.11.0` - PDF text extraction
- `PyMuPDF==1.26.3` - Alternative PDF processing

**External Integrations**:
- `openai==1.97.0` - OpenAI API client
- `supabase==2.17.0` - Supabase Python client
- `requests==2.32.4` - HTTP requests

**Utilities**:
- `asyncio==0.0.2` - Asynchronous operations support

### Environment Configuration

Required environment variables:
- `OPENAI_API_KEY` - OpenAI authentication
- `SUPABASE_URL` - Supabase project URL
- `SUPABASE_KEY` - Supabase service role key

### File Storage

**Supabase Storage Buckets**:
- `user-uploads/` - Stores uploaded Excel and PDF files
- Path structure: `{user_id}/{filename}`

### Security Considerations

- API rate limiting prevents abuse
- Environment-based credential management
- CORS configured for cross-origin requests
- Input validation on file paths and user IDs