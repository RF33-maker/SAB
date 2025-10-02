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

**Excel Parsing**:
- **Technology**: Pandas for structured data processing
- **Process**: Field mapping system translates various column naming conventions to standardized schema
- **Normalization**: Handles case-insensitive field matching and multiple naming variants
- **Pool Support**: Optional "Pool" column for leagues with multiple pools (e.g., NBL Division 1). Automatically detected and stored in game_schedule when present; gracefully skipped for leagues without pools
- **Schedule-First Processing**: Games are added to game_schedule immediately from Excel data, then LiveStats data is fetched if available. This enables future/unplayed games to appear in schedules while stats are processed separately for completed games

**Data Validation**: Player name normalization removes captain designations (C) and handles bracket variations for consistent database queries

### Data Storage

**Database**: Supabase (PostgreSQL-based)

**Schema Design**:
- `player_stats` table stores individual game performance records
- Key fields: player name, game date, team, opponent, comprehensive statistics (points, rebounds, assists, shooting percentages, etc.)
- League ID support for multi-league data isolation

**Rationale**: Supabase provides a managed PostgreSQL database with built-in REST API, authentication, and real-time capabilities. The schema is denormalized for query performance, storing all stats in a single table rather than normalized relations.

### AI Integration

**OpenAI Assistant API**:
- Persistent assistant with custom function calling capabilities
- Thread-based conversation management for context retention
- In-memory caching for delayed responses and player data

**Custom Tools/Functions**:
- `get_player_stats` - Retrieves specific statistics for players
- `get_top_players` - Identifies league leaders in various categories  
- `get_game_summary` - Provides AI-generated game analysis
- `get_team_analysis` - Analyzes team performance patterns
- `get_advanced_insights` - Calculates advanced metrics
- `get_player_trending` - Identifies performance trends

**Design Decision**: Function calling approach allows the AI to directly query the database with structured parameters, providing accurate data-driven responses rather than hallucinated statistics.

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