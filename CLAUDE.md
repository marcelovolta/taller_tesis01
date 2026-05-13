# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project purpose

Predict commercial success/failure of movie releases from movie metadata and YouTube comments left on the official trailer. The pipeline pulls movies from TMDB, finds the official YouTube trailer for each, downloads comments published *strictly before the release date* (to avoid data leakage during prediction), and stores everything in PostgreSQL for downstream dbt modeling.

## Running the pipeline

The codebase is designed to run inside the dev container (`.devcontainer/`). PostgreSQL runs as a sibling service (`postgres`) and the Python workspace is the container the user codes from.

```bash
# Inside the workspace container, from /workspace/python:
python main.py
```

`main.py` is the single entrypoint. The TMDB ingestion (`get_tmdb_movies_us` / `load_to_postgres`) is currently commented out — the live call is `process_trailer_comments(reset=False)`. Toggle the commented blocks to re-run the movie-ingestion step. Pass `reset=True` to truncate the comments and progress tables and reprocess everything from scratch — otherwise the run *resumes* by skipping movies already marked `success` in `trailer_comments_progress`.

Logs are written to `python/logs/log_<timestamp>.log` (also streamed to stdout).

## dbt

```bash
# From /workspace (repo root):
dbt deps      # installs packages — runs automatically as devcontainer postCreateCommand
dbt run       # runs models in models/
dbt test
dbt run --select <model_name>
```

`dbt_project.yml` declares the `movie_success` project with `+materialized: view` as the default. The dbt profile (`.dbt/profiles.yml`) targets the `postgres` service inside the compose network — `DBT_PROFILES_DIR=/workspace/.dbt` is set in `docker-compose.yml`.

## Database access

- **From the host machine:** `localhost:5433` (mapped in `docker-compose.yml`).
- **From inside the workspace container:** host `postgres`, port `5432`.
- Credentials: `dbt_user` / `dbt_pass`, database `analytics`, schema `public`.
- `python/config.yaml` uses the in-container values (`POSTGRE_HOST: postgres`, port `5432`).

Backup/restore commands (run from the host) are documented in `readme.md`.

## Configuration split

Settings are deliberately split across two files:

- `python/.env` — **secrets only** (TMDB tokens, YouTube API key, Postgres credentials). Loaded via `pydantic_settings.BaseSettings` in `python/src/config.py`.
- `python/config.yaml` — non-sensitive settings (project name, date window, DB host/port/name/schema, target table name).

Both are exposed as module-level constants on `src.config`. Other modules import via `from src import config` and read `config.<NAME>`. **Do not put secrets in the YAML.**

The working directory matters: `config_path = "./config.yaml"` is relative, so `main.py` must be run from `python/`.

## Architecture

Two ingestion stages, both writing to Postgres:

1. **Movie metadata (`src/movie_database.py`)** — `get_tmdb_movies_us()` paginates TMDB Discover, calls `get_movie_details()` for each result (which appends `credits,videos,release_dates`), and extracts the official YouTube trailer key. `load_to_postgres()` coerces dtypes (nullable Int64 for ints, NaN→None for strings) and writes in 100-row chunks with a row-by-row fallback for any chunk that fails — this is intentional: TMDB returns dirty data and the fallback isolates the bad rows. Target table: `public.movies_2025` (configurable via `TABLE_NAME`).

2. **Trailer comments (`src/trailer_comments.py`)** — orchestrated by `process_trailer_comments()`. For each pending movie:
   - `find_trailers()` searches YouTube for `"<title>" official trailer` and keeps only results whose normalized title starts with `<title> official trailer`.
   - The `youtube_trailer_key` from TMDB is merged in if not already found, so we always try the canonical trailer.
   - `get_comments()` paginates `commentThreads.list` and **drops every comment with `published_at >= release_date`** — this leakage filter is core to the prediction premise.
   - Results are upserted into `trailer_comments` (PK `comment_id`, `ON CONFLICT DO NOTHING`). Movies are marked in `trailer_comments_progress` with status `success` / `no_trailers` / `error`.

### Resumability and quota handling

The progress table is the source of truth for "what's done". Quota-exceeded errors from YouTube (`_is_quota_error`) propagate up — the orchestrator `break`s out of the movie loop **without** marking the current movie as processed, so the next run picks up exactly where the quota ran out. Other 403s (e.g. comments disabled) are logged and the trailer is skipped. When editing this code, preserve the invariant: **a movie is marked `success` only after every trailer's comments are saved** — partial progress on a movie should leave it unmarked so the next run retries it.

## Conventions

- Mixed Spanish/English in logs and comments — match what's already in the file you're editing rather than rewriting.
- Module-level `logger = logging.getLogger(__name__)` is the only logging pattern; `main.py` configures the root logger.
- `from src import *` in `main.py` re-exports everything via `src/__init__.py`. New modules should be added to `src/__init__.py` if they expose top-level functions main.py should call.
