from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
import pandas as pd
from sqlalchemy import create_engine, text
from src import config
import logging
import datetime
import re
import time

logger = logging.getLogger(__name__)

API_KEY = config.YOUTUBE_API_KEY
youtube = build("youtube", "v3", developerKey=API_KEY)

POSTGRE_USER = config.POSTGRE_USER
POSTGRE_PASS = config.POSTGRE_PASS
POSTGRE_HOST = config.POSTGRE_HOST
POSTGRE_PORT = config.POSTGRE_PORT
DB_NAME = config.DB_NAME
DB_SCHEMA = config.DB_SCHEMA

MOVIES_TABLE = "movies_2025"
COMMENTS_TABLE = "trailer_comments"
PROGRESS_TABLE = "trailer_comments_progress"


# ----
# Database helpers
# ----

def get_engine():
    return create_engine(
        f"postgresql+psycopg2://{POSTGRE_USER}:{POSTGRE_PASS}@{POSTGRE_HOST}:{POSTGRE_PORT}/{DB_NAME}"
    )


def ensure_tables_exist(engine):
    """Create comments and progress tables if they don't exist yet."""
    with engine.begin() as conn:
        conn.execute(text(f"""
            CREATE TABLE IF NOT EXISTS {DB_SCHEMA}.{COMMENTS_TABLE} (
                tmdb_id             BIGINT,
                video_id            TEXT,
                comment_id          TEXT PRIMARY KEY,
                author_name         TEXT,
                author_channel_id   TEXT,
                text                TEXT,
                published_at        TIMESTAMPTZ,
                like_count          INTEGER
            );
        """))
        conn.execute(text(f"""
            CREATE TABLE IF NOT EXISTS {DB_SCHEMA}.{PROGRESS_TABLE} (
                tmdb_id         BIGINT PRIMARY KEY,
                processed_at    TIMESTAMPTZ DEFAULT now(),
                status          TEXT,
                notes           TEXT
            );
        """))
    logger.info("Tables verified/created.")


def reset_progress(engine):
    """
    Clears the progress table and all saved comments so the process
    can be restarted from scratch.
    """
    with engine.begin() as conn:
        conn.execute(text(f"TRUNCATE TABLE {DB_SCHEMA}.{PROGRESS_TABLE};"))
        conn.execute(text(f"TRUNCATE TABLE {DB_SCHEMA}.{COMMENTS_TABLE};"))
    logger.info("Progress and comments tables cleared. Ready to reprocess from scratch.")


def get_pending_movies(engine):
    """
    Returns movies with runtime > 60 and release_date >= 2024-01-01
    that have NOT been successfully processed yet.
    """
    query = text(f"""
        SELECT m.tmdb_id, m.title, m.release_date, m.youtube_trailer_key
        FROM {DB_SCHEMA}.{MOVIES_TABLE} m
        LEFT JOIN {DB_SCHEMA}.{PROGRESS_TABLE} p ON m.tmdb_id = p.tmdb_id
        WHERE m.runtime > 60
          AND m.release_date >= '2024-01-01'
          AND (p.tmdb_id IS NULL OR p.status NOT IN ('success', 'no_trailers'))
        ORDER BY m.release_date ASC
    """)
    with engine.connect() as conn:
        return pd.read_sql(query, conn)


def mark_movie_processed(engine, tmdb_id: int, status: str, notes: str = None):
    """Upsert a row in the progress table."""
    with engine.begin() as conn:
        conn.execute(text(f"""
            INSERT INTO {DB_SCHEMA}.{PROGRESS_TABLE} (tmdb_id, processed_at, status, notes)
            VALUES (:tmdb_id, now(), :status, :notes)
            ON CONFLICT (tmdb_id) DO UPDATE
                SET processed_at = now(),
                    status = EXCLUDED.status,
                    notes = EXCLUDED.notes
        """), {"tmdb_id": int(tmdb_id), "status": status, "notes": notes})


def save_comments(engine, tmdb_id: int, comments: list[dict]) -> int:
    """
    Saves a list of comment dicts to the database row by row.
    Skips duplicate comment_ids via ON CONFLICT DO NOTHING.
    Returns the number of rows successfully saved.
    """
    if not comments:
        logger.info(f"No comments to save for tmdb_id={tmdb_id}.")
        return 0

    df = _clean_comments_df(pd.DataFrame(comments), tmdb_id)

    if df.empty:
        logger.warning(f"All comments were invalid for tmdb_id={tmdb_id}.")
        return 0

    saved = 0
    for _, row in df.iterrows():
        try:
            with engine.begin() as conn:
                conn.execute(text(f"""
                    INSERT INTO {DB_SCHEMA}.{COMMENTS_TABLE}
                    (tmdb_id, video_id, comment_id, author_name,
                    author_channel_id, text, published_at, like_count)
                    VALUES
                    (:tmdb_id, :video_id, :comment_id, :author_name,
                    :author_channel_id, :text, :published_at, :like_count)
                    ON CONFLICT (comment_id) DO NOTHING
                """), row.to_dict())
            saved += 1
        except Exception as e:
            logger.error(f"Failed to save comment {row.get('comment_id')}: {e}")

    logger.info(f"Saved {saved}/{len(df)} comments for tmdb_id={tmdb_id}.")
    return saved


def _clean_comments_df(df: pd.DataFrame, tmdb_id: int) -> pd.DataFrame:
    """Coerce types and drop rows that would cause DB errors."""
    df = df.copy()
    df["tmdb_id"] = int(tmdb_id)

    # Drop rows without a comment_id — can't be primary key
    df = df[df["comment_id"].notna() & (df["comment_id"] != "")]

    # Coerce published_at to timezone-aware timestamp
    df["published_at"] = pd.to_datetime(df["published_at"], errors="coerce", utc=True)

    # Coerce like_count to nullable int
    df["like_count"] = pd.to_numeric(df["like_count"], errors="coerce")
    df["like_count"] = df["like_count"].where(df["like_count"].notna(), other=None)
    df["like_count"] = df["like_count"].astype("Int64")

    # Truncate text fields to avoid oversized payloads
    for col in ["author_name", "text"]:
        df[col] = df[col].apply(lambda x: str(x)[:1000] if pd.notna(x) else None)

    for col in ["video_id", "comment_id", "author_channel_id"]:
        df[col] = df[col].apply(lambda x: str(x) if pd.notna(x) else None)

    return df


# ----
# YouTube helpers
# ----

def normalize_title(title: str) -> str:
    title = title.lower()
    title = re.sub(r"\([^)]*\)", "", title)
    title = re.sub(r"[^a-z0-9 ]+", " ", title)
    title = re.sub(r"\s+", " ", title).strip()
    return title


def _is_quota_error(e: HttpError) -> bool:
    """Returns True if the HttpError is a YouTube quota exceeded error."""
    if e.resp.status != 403:
        return False
    try:
        details = e.error_details or []
        for detail in details:
            if detail.get("reason") == "quotaExceeded":
                return True
    except Exception:
        pass
    return "quota" in str(e).lower()


def find_trailers(movie_title: str, max_results: int = 10) -> list[dict]:
    """
    Searches YouTube for official trailers matching movie_title.
    Raises HttpError if quota is exceeded.
    Returns a list of dicts with video metadata.
    """
    query = f'"{movie_title}" official trailer'
    try:
        res = youtube.search().list(
            part="snippet",
            q=query,
            type="video",
            maxResults=max_results,
            regionCode="US",
            relevanceLanguage="en"
        ).execute()
    except HttpError as e:
        if _is_quota_error(e):
            logger.warning(f"Quota exceeded during trailer search for '{movie_title}'. Re-raising.")
            raise
        logger.error(f"YouTube search failed for '{movie_title}': {e}")
        return []

    videos = []
    movie_title_norm = normalize_title(movie_title)

    for item in res.get("items", []):
        video_id = item["id"].get("videoId")
        if not video_id:
            continue
        returned_title = item["snippet"].get("title", "")
        channel = item["snippet"].get("channelTitle", "")
        returned_title_norm = normalize_title(returned_title)
        
        expected_prefix = normalize_title(f"{movie_title_norm} official trailer")
        is_likely_match = returned_title_norm.startswith(expected_prefix)
        
        if is_likely_match:
            videos.append({
                "video_id": video_id,
                "title": returned_title,
                "channel": channel,
                "published_at": item["snippet"].get("publishedAt"),
                "url": f"https://www.youtube.com/watch?v={video_id}"
            })

    return videos


def get_comments(
    video_id: str,
    limit_date: datetime.date = None,
    max_pages: int = None
) -> list[dict]:
    """
    Retrieves comments for a YouTube video published strictly before limit_date.
    Raises HttpError if quota is exceeded so the caller can stop the session.
    Returns a list of comment dicts.
    """
    rows = []
    req = youtube.commentThreads().list(
        part="snippet",
        videoId=video_id,
        maxResults=100,
        textFormat="plainText",
        order="time"
    )

    pages = 0
    logger.info(f"Fetching comments for video={video_id}, limit_date={limit_date}")

    while req is not None and (max_pages is None or pages < max_pages):
        try:
            res = req.execute()
        except HttpError as e:
            if _is_quota_error(e):
                logger.warning(f"Quota exceeded while fetching comments for {video_id}. Re-raising.")
                raise  # propagate to orchestrator — do NOT mark movie as processed
            elif e.resp.status == 403:
                logger.warning(f"Comments disabled for video {video_id}: {e}")
                break  # comments turned off — skip video, don't raise
            else:
                logger.error(f"HTTP error on page {pages} for video {video_id}: {e}")
                break

        for item in res.get("items", []):
            try:
                top = item["snippet"]["topLevelComment"]["snippet"]
                published_at = pd.to_datetime(top.get("publishedAt"), utc=True)

                # Only keep comments strictly before the release date (avoid data leakage)
                if limit_date is not None and published_at.date() >= limit_date:
                    continue

                rows.append({
                    "video_id": video_id,
                    "comment_id": item["snippet"]["topLevelComment"].get("id"),
                    "author_name": top.get("authorDisplayName"),
                    "author_channel_id": top.get("authorChannelId", {}).get("value"),
                    "text": top.get("textDisplay"),
                    "published_at": top.get("publishedAt"),
                    "like_count": top.get("likeCount"),
                })
            except Exception as e:
                logger.warning(f"Skipping malformed comment item: {e}")
                continue

        req = youtube.commentThreads().list_next(req, res)
        pages += 1
        time.sleep(0.1)

    logger.info(f"Retrieved {len(rows)} comments for video={video_id}")
    return rows


# ----
# Main orchestration
# ----

def process_trailer_comments(reset: bool = False):
    """
    Entry point for main module.

    Args:
        reset: If True, clears all progress and comments and reprocesses
               everything from scratch. Use this when restarting the process.

    Reads pending movies from the DB, fetches trailer comments from YouTube,
    and saves results. Resumes automatically on subsequent runs unless reset=True.
    """
    engine = get_engine()
    ensure_tables_exist(engine)

    if reset:
        logger.info("Reset requested — clearing progress and comments tables.")
        reset_progress(engine)

    pending = get_pending_movies(engine)
    logger.info(f"{len(pending)} movies pending processing.")

    if pending.empty:
        logger.info("No pending movies. All done.")
        return

    for _, movie in pending.iterrows():
        tmdb_id = movie["tmdb_id"]
        title = movie["title"]
        release_date = movie["release_date"]
        db_trailer_key = movie.get("youtube_trailer_key")

        logger.info(f"Processing tmdb_id={tmdb_id} | '{title}' | release={release_date}")

        # --- Find trailers ---
        try:
            trailers = find_trailers(title)
        except HttpError as e:
            if _is_quota_error(e):
                logger.warning("Quota exceeded during trailer search. Stopping session.")
                break  # stop entirely — do NOT mark movie as processed
            logger.error(f"Unexpected error finding trailers for '{title}': {e}")
            mark_movie_processed(engine, tmdb_id, "error", str(e))
            continue

        # Merge DB trailer key if not already found
        found_ids = {t["video_id"] for t in trailers}
        if db_trailer_key and db_trailer_key not in found_ids:
            trailers.append({
                "video_id": db_trailer_key,
                "title": f"{title} (from DB)",
                "channel": None,
                "published_at": None,
                "url": f"https://www.youtube.com/watch?v={db_trailer_key}"
            })

        if not trailers:
            logger.warning(f"No trailers found for '{title}' (tmdb_id={tmdb_id}).")
            mark_movie_processed(engine, tmdb_id, "no_trailers")
            continue

        # --- Fetch and save comments per trailer ---
        total_saved = 0
        quota_exceeded = False

        for trailer in trailers:
            video_id = trailer["video_id"]
            try:
                comments = get_comments(video_id, limit_date=release_date)
            except HttpError as e:
                if _is_quota_error(e):
                    logger.warning("Quota exceeded while fetching comments. Stopping session.")
                    quota_exceeded = True
                    break  # stop trailer loop
                logger.error(f"Error fetching comments for video {video_id}: {e}")
                continue

            saved = save_comments(engine, tmdb_id, comments)
            total_saved += saved
            time.sleep(0.25)

        if quota_exceeded:
            # Exit movie loop without marking this movie as processed
            break

        # Only mark success AFTER all comments are saved successfully
        mark_movie_processed(
            engine, tmdb_id, "success",
            f"{len(trailers)} trailers, {total_saved} comments saved"
        )
        logger.info(f"Completed tmdb_id={tmdb_id}: {total_saved} comments saved.")
        time.sleep(0.5)

    logger.info("Session complete.")