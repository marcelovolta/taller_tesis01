import requests
from bs4 import BeautifulSoup
import pandas as pd
from sqlalchemy import create_engine, text
from src import config
import logging
import re
import time
import unicodedata

logger = logging.getLogger(__name__)

POSTGRE_USER = config.POSTGRE_USER
POSTGRE_PASS = config.POSTGRE_PASS
POSTGRE_HOST = config.POSTGRE_HOST
POSTGRE_PORT = config.POSTGRE_PORT
DB_NAME = config.DB_NAME
DB_SCHEMA = config.DB_SCHEMA

MOVIES_TABLE = "movies_2025"
BOX_OFFICE_TABLE = "box_office"
PROGRESS_TABLE = "box_office_progress"

BASE_URL = "https://www.the-numbers.com"
REQUEST_DELAY = 1.0  # seconds between HTTP requests — polite rate limit
HTTP_TIMEOUT = 15
USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/120.0.0.0 Safari/537.36"
)

session = requests.Session()
session.headers.update({"User-Agent": USER_AGENT})


# ----
# Database helpers
# ----

def get_engine():
    return create_engine(
        f"postgresql+psycopg2://{POSTGRE_USER}:{POSTGRE_PASS}@{POSTGRE_HOST}:{POSTGRE_PORT}/{DB_NAME}"
    )


def ensure_tables_exist(engine):
    """Create box-office and progress tables if they don't exist yet."""
    with engine.begin() as conn:
        conn.execute(text(f"""
            CREATE TABLE IF NOT EXISTS {DB_SCHEMA}.{BOX_OFFICE_TABLE} (
                tmdb_id                 BIGINT PRIMARY KEY,
                the_numbers_url         TEXT,
                opening_weekend_revenue BIGINT,
                production_budget       BIGINT,
                opening_theaters        INTEGER,
                scraped_at              TIMESTAMPTZ DEFAULT now()
            );
        """))
        # Migrate older deployments that pre-date the opening_theaters column.
        conn.execute(text(f"""
            ALTER TABLE {DB_SCHEMA}.{BOX_OFFICE_TABLE}
            ADD COLUMN IF NOT EXISTS opening_theaters INTEGER;
        """))
        conn.execute(text(f"""
            CREATE TABLE IF NOT EXISTS {DB_SCHEMA}.{PROGRESS_TABLE} (
                tmdb_id         BIGINT PRIMARY KEY,
                processed_at    TIMESTAMPTZ DEFAULT now(),
                status          TEXT,
                notes           TEXT
            );
        """))
    logger.info("Box-office tables verified/created.")


def reset_progress(engine):
    """Clears the progress and box-office tables to reprocess from scratch."""
    with engine.begin() as conn:
        conn.execute(text(f"TRUNCATE TABLE {DB_SCHEMA}.{PROGRESS_TABLE};"))
        conn.execute(text(f"TRUNCATE TABLE {DB_SCHEMA}.{BOX_OFFICE_TABLE};"))
    logger.info("Box-office progress and data tables cleared.")


def get_pending_movies(engine):
    """
    Returns movies with runtime > 60 and release_date >= 2024-01-01
    that have NOT been successfully processed yet, excluding terminal
    'not_found' rows so they are not retried.
    """
    query = text(f"""
        SELECT m.tmdb_id, m.title, m.release_date
        FROM {DB_SCHEMA}.{MOVIES_TABLE} m
        LEFT JOIN {DB_SCHEMA}.{PROGRESS_TABLE} p ON m.tmdb_id = p.tmdb_id
        WHERE m.runtime > 60
          AND (p.tmdb_id IS NULL OR p.status NOT IN ('success', 'not_found'))
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


def save_box_office(
    engine,
    tmdb_id: int,
    url: str,
    opening_weekend: int | None,
    production_budget: int | None,
    opening_theaters: int | None,
):
    """Upsert a box-office row for a movie."""
    with engine.begin() as conn:
        conn.execute(text(f"""
            INSERT INTO {DB_SCHEMA}.{BOX_OFFICE_TABLE}
                (tmdb_id, the_numbers_url, opening_weekend_revenue,
                 production_budget, opening_theaters, scraped_at)
            VALUES (:tmdb_id, :url, :ow, :pb, :ot, now())
            ON CONFLICT (tmdb_id) DO UPDATE
                SET the_numbers_url = EXCLUDED.the_numbers_url,
                    opening_weekend_revenue = EXCLUDED.opening_weekend_revenue,
                    production_budget = EXCLUDED.production_budget,
                    opening_theaters = EXCLUDED.opening_theaters,
                    scraped_at = now()
        """), {
            "tmdb_id": int(tmdb_id),
            "url": url,
            "ow": int(opening_weekend) if opening_weekend is not None else None,
            "pb": int(production_budget) if production_budget is not None else None,
            "ot": int(opening_theaters) if opening_theaters is not None else None,
        })


# ----
# Scraping helpers
# ----

def _parse_money(raw: str) -> int | None:
    """Parse a '$X,XXX,XXX' string into an int. Returns None for unknown/missing."""
    if not raw:
        return None
    raw = raw.strip()
    if raw.lower() in ("unknown", "n/a", "-", "—", ""):
        return None
    match = re.search(r"\$?([\d,]+)", raw)
    if not match:
        return None
    digits = match.group(1).replace(",", "")
    try:
        return int(digits)
    except ValueError:
        return None


def _release_year(release_date) -> int | None:
    """Extract a 4-digit year from a date/Timestamp/str. Returns None if absent."""
    if release_date is None or pd.isna(release_date):
        return None
    if hasattr(release_date, "year"):
        return int(release_date.year)
    s = str(release_date)
    match = re.search(r"(\d{4})", s)
    return int(match.group(1)) if match else None


def _strip_accents(s: str) -> str:
    """Decompose Unicode and drop combining marks so 'Niño' becomes 'Nino'."""
    nfkd = unicodedata.normalize("NFKD", s)
    return "".join(c for c in nfkd if not unicodedata.combining(c))


def _make_slug(title: str) -> str:
    """
    Build a the-numbers.com URL slug from a movie title.
    Convention: ASCII letters/digits only, spaces become hyphens, case preserved.
    Example: "El Niño: The Movie" -> "El-Nino-The-Movie".
    """
    s = _strip_accents(title)
    s = re.sub(r"[^A-Za-z0-9\s-]", "", s)
    s = re.sub(r"\s+", "-", s.strip())
    s = re.sub(r"-+", "-", s)
    return s


def candidate_urls(title: str, release_year: int | None = None) -> list[str]:
    """
    Returns the candidate the-numbers.com movie URLs for a title, year-tagged
    first (the disambiguated form, e.g. '/movie/Only-Child-(2024)') and the
    bare slug as a fallback (e.g. '/movie/Avengers-Endgame').
    """
    slug = _make_slug(title)
    if not slug:
        return []
    urls = []
    if release_year is not None:
        urls.append(f"{BASE_URL}/movie/{slug}-({release_year})")
    urls.append(f"{BASE_URL}/movie/{slug}")
    return urls


def fetch_movie_page(url: str) -> dict | None:
    """
    Fetches a the-numbers.com movie page and extracts:
      - opening_weekend_revenue (3-day domestic, USD)
      - production_budget (USD)
    Returns a dict on HTTP 200 (values may be None if absent on the page).
    Returns None on HTTP 404 — the page does not exist.
    Raises requests.RequestException on other network errors so the caller
    can decide whether to retry.
    """
    res = session.get(url, timeout=HTTP_TIMEOUT)
    if res.status_code == 404:
        return None
    res.raise_for_status()

    soup = BeautifulSoup(res.text, "html.parser")
    data = {
        "opening_weekend_revenue": None,
        "production_budget": None,
        "opening_theaters": None,
    }

    # The summary stats live in table rows: <td>Label:</td><td>$value</td>.
    # The labels use &nbsp; between words ("Opening&nbsp;Weekend"), so we
    # collapse all whitespace (including U+00A0) before matching.
    for row in soup.find_all("tr"):
        cells = row.find_all("td")
        if len(cells) < 2:
            continue
        label = re.sub(r"\s+", " ", cells[0].get_text(" ", strip=True)).rstrip(":").lower()
        value_text = cells[1].get_text(" ", strip=True)

        if label == "production budget":
            data["production_budget"] = _parse_money(value_text)
        elif label == "opening weekend":
            data["opening_weekend_revenue"] = _parse_money(value_text)
        elif label == "theater counts":
            # Cell looks like: "4,125 opening theaters/4,125 max. theaters, ..."
            # _parse_money grabs the first comma-separated integer in the string.
            data["opening_theaters"] = _parse_money(value_text)

    return data


# ----
# Main orchestration
# ----

def process_box_office(reset: bool = False):
    """
    Entry point for main module.

    Args:
        reset: If True, clears all progress and box-office data and reprocesses
               everything from scratch. Use this when restarting the process.

    Reads pending movies from the DB, scrapes the-numbers.com for opening
    weekend revenue and production budget, and saves results. Resumes
    automatically on subsequent runs unless reset=True.
    """
    engine = get_engine()
    ensure_tables_exist(engine)

    if reset:
        logger.info("Reset requested — clearing box-office progress and data.")
        reset_progress(engine)

    pending = get_pending_movies(engine)
    logger.info(f"{len(pending)} movies pending box-office processing.")

    if pending.empty:
        logger.info("No pending movies. All done.")
        return

    for _, movie in pending.iterrows():
        tmdb_id = movie["tmdb_id"]
        title = movie["title"]
        release_date = movie["release_date"]
        year = _release_year(release_date)

        logger.info(f"Processing tmdb_id={tmdb_id} | '{title} ({year})' | release={release_date}")

        urls = candidate_urls(title, release_year=year)
        if not urls:
            logger.warning(f"Could not build a slug for '{title}' (tmdb_id={tmdb_id}).")
            mark_movie_processed(engine, tmdb_id, "not_found", "empty slug")
            continue

        matched_url = None
        data = None
        last_error = None

        for url in urls:
            try:
                result = fetch_movie_page(url)
            except requests.RequestException as e:
                last_error = f"{url}: {e}"
                logger.error(f"Network error fetching {url}: {e}")
                time.sleep(REQUEST_DELAY)
                continue

            time.sleep(REQUEST_DELAY)

            if result is None:
                logger.info(f"404 for {url}, trying next candidate if any.")
                continue

            matched_url = url
            data = result
            break

        if data is None:
            if last_error is not None:
                mark_movie_processed(engine, tmdb_id, "error", last_error)
            else:
                logger.warning(f"No the-numbers page found for '{title}' (tmdb_id={tmdb_id}). Tried: {urls}")
                mark_movie_processed(engine, tmdb_id, "not_found", f"tried: {urls}")
            continue

        save_box_office(
            engine,
            tmdb_id,
            matched_url,
            data["opening_weekend_revenue"],
            data["production_budget"],
            data["opening_theaters"],
        )

        notes = (
            f"opening_weekend={data['opening_weekend_revenue']}, "
            f"production_budget={data['production_budget']}, "
            f"opening_theaters={data['opening_theaters']}, url={matched_url}"
        )
        mark_movie_processed(engine, tmdb_id, "success", notes)
        logger.info(f"Completed tmdb_id={tmdb_id}: {notes}")

    logger.info("Box-office session complete.")
