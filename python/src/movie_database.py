import os
import time
import requests
import pandas as pd
from sqlalchemy import create_engine, text
from src import config
import logging

logger = logging.getLogger(__name__)

TMDB_API_READ_ACCESS_TOKEN = config.TMDB_API_READ_ACCESS_TOKEN
TMDB_API_KEY = config.TMDB_API_KEY
POSTGRE_USER = config.POSTGRE_USER
POSTGRE_PASS = config.POSTGRE_PASS
POSTGRE_HOST = config.POSTGRE_HOST
POSTGRE_PORT = config.POSTGRE_PORT
DB_NAME = config.DB_NAME
DB_SCHEMA = config.DB_SCHEMA
TABLE_NAME = config.TABLE_NAME
START_DATE = config.START_DATE
END_DATE = config.END_DATE


def get_tmdb_movies_us(max_pages=None):
    if not TMDB_API_READ_ACCESS_TOKEN:
        raise ValueError("Falta definir TMDB_API_READ_ACCESS_TOKEN como variable de entorno.")

    rows = []
    page = 1

    logger.info("Iniciando recueperación de películas")
    while True:
        url = "https://api.themoviedb.org/3/discover/movie"

        params = {
            "api_key": TMDB_API_KEY,
            "language": "en-US",
            "region": "US",
            "release_date.gte": START_DATE,
            "release_date.lte": END_DATE,
            "with_release_type": "2|3",
            "sort_by": "release_date.asc",
            "page": page,
        }

        res = requests.get(url, params=params, timeout=30)
        res.raise_for_status()
        data = res.json()

        for movie in data.get("results", []):
            details = get_movie_details(movie["id"])
            
            rows.append({
                "tmdb_id": details.get("id"),
                "imdb_id": details.get("imdb_id"),
                "title": details.get("title"),
                "original_title": details.get("original_title"),
                "release_date": details.get("release_date"),
                "original_language": details.get("original_language"),
                "origin_country": ", ".join(details.get("origin_country", [])),
                "production_countries": ", ".join(
                    c.get("name", "") for c in details.get("production_countries", [])
                ),
                "production_companies": ", ".join(
                    c.get("name", "") for c in details.get("production_companies", [])
                ),
                "genres": ", ".join(
                    g.get("name", "") for g in details.get("genres", [])
                ),
                "budget": details.get("budget"),
                "revenue": details.get("revenue"),
                "runtime": details.get("runtime"),
                "overview": details.get("overview"),
                "popularity": details.get("popularity"),
                "vote_average": details.get("vote_average"),
                "vote_count": details.get("vote_count"),
                "cast_top_10": ", ".join(
                    c.get("name", "")
                    for c in details.get("credits", {}).get("cast", [])[:10]
                ),
                "directors": ", ".join(
                    c.get("name", "")
                    for c in details.get("credits", {}).get("crew", [])
                    if c.get("job") == "Director"
                ),
                "youtube_trailer_key": get_youtube_trailer_key(details),
            })

            time.sleep(0.25)

        total_pages = data.get("total_pages", 1)

        print(f"Página {page}/{total_pages} procesada")

        if page >= total_pages:
            break

        if max_pages is not None and page >= max_pages:
            break

        page += 1
        time.sleep(0.25)

    return pd.DataFrame(rows)


def get_movie_details(tmdb_id):
    url = f"https://api.themoviedb.org/3/movie/{tmdb_id}"

    params = {
        "api_key": TMDB_API_KEY,
        "language": "en-US",
        "append_to_response": "credits,videos,release_dates",
    }

    try:
        res = requests.get(url, params=params, timeout=30)
        res.raise_for_status()
        logger.info(f"Recuperados los detalles de la pelicula: {tmdb_id}")
        data = res.json()
    
    except requests.exceptions.RequestException as e:
        logger.error(f"Error al recuperar los detalles de la pelicula {tmdb_id} \
                    Error reportado: {e}")
        data = None
    
    return data


def get_youtube_trailer_key(details):
    if details is None:
        return None
    videos = details.get("videos", {}).get("results", [])

    for video in videos:
        if (
            video.get("site") == "YouTube"
            and video.get("type") == "Trailer"
            and video.get("official") is True
        ):
            return video.get("key")

    for video in videos:
        if (
            video.get("site") == "YouTube"
            and video.get("type") == "Trailer"
        ):
            return video.get("key")

    return None


def load_to_postgres(df):
    df = df.copy()

    # --- Dates ---
    df["release_date"] = df["release_date"].replace("", pd.NA)
    df["release_date"] = pd.to_datetime(df["release_date"], errors="coerce").dt.date

    # --- Numeric fields: coerce to expected types, replace NaN with None ---
    int_fields = ["tmdb_id", "budget", "revenue", "runtime", "vote_count"]
    float_fields = ["popularity", "vote_average"]

    for col in int_fields:
        df[col] = pd.to_numeric(df[col], errors="coerce")
        df[col] = df[col].where(df[col].notna(), other=None)
        df[col] = df[col].astype("Int64")  # nullable integer

    for col in float_fields:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    # --- String fields: ensure they are str, replace NaN/None with None ---
    str_fields = [
        "imdb_id", "title", "original_title", "original_language",
        "origin_country", "production_countries", "production_companies",
        "genres", "overview", "cast_top_10", "directors", "youtube_trailer_key"
    ]

    for col in str_fields:
        df[col] = df[col].where(df[col].notna(), other=None)
        df[col] = df[col].apply(lambda x: str(x) if x is not None else None)

    # --- Log a sample before loading ---
    logger.info(f"dtypes before load:\n{df.dtypes}")
    logger.info(f"Sample row:\n{df.iloc[0].to_dict() if len(df) > 0 else 'empty'}")

    engine = create_engine(
        f"postgresql+psycopg2://{POSTGRE_USER}:{POSTGRE_PASS}@{POSTGRE_HOST}:{POSTGRE_PORT}/{DB_NAME}"
    )

    # --- Load in chunks with per-chunk error reporting ---
    chunk_size = 100
    total = len(df)
    failed_chunks = []

    for start in range(0, total, chunk_size):
        chunk = df.iloc[start:start + chunk_size]
        try:
            chunk.to_sql(
                TABLE_NAME,
                engine,
                schema=DB_SCHEMA,
                if_exists="append",
                index=False,
                method="multi",
                chunksize=chunk_size,
            )
            logger.info(f"Loaded rows {start} to {min(start + chunk_size, total)}")
        except Exception as e:
            logger.error(f"Failed chunk {start}-{start + chunk_size}: {e}")
            logger.error(f"Problematic chunk sample:\n{chunk.head(2).to_dict()}")
            failed_chunks.append((start, chunk))

    if failed_chunks:
        logger.warning(f"{len(failed_chunks)} chunks failed. Attempting row-by-row fallback...")
        for start, chunk in failed_chunks:
            for idx, row in chunk.iterrows():
                try:
                    pd.DataFrame([row]).to_sql(
                        TABLE_NAME,
                        engine,
                        schema=DB_SCHEMA,
                        if_exists="append",
                        index=False,
                        method="multi",
                    )
                except Exception as e:
                    logger.error(f"Failed row {idx} (tmdb_id={row.get('tmdb_id')}): {e}")
                    logger.error(f"Row data: {row.to_dict()}")
                    

def load_to_postgres_deprecated(df):
    df["release_date"] = df["release_date"].replace("", pd.NA)
    df["release_date"] = pd.to_datetime(df["release_date"], errors="coerce").dt.date
    engine = create_engine(
        f"postgresql+psycopg2://{POSTGRE_USER}:{POSTGRE_PASS}@{POSTGRE_HOST}:{POSTGRE_PORT}/{DB_NAME}"
    )

    # with engine.begin() as conn:
        # conn.execute(text(f"""
        #     DROP TABLE IF EXISTS {DB_SCHEMA}.{TABLE_NAME};

        #     CREATE TABLE {DB_SCHEMA}.{TABLE_NAME} (
        #         tmdb_id BIGINT PRIMARY KEY,
        #         imdb_id TEXT,
        #         title TEXT,
        #         original_title TEXT,
        #         release_date DATE,
        #         original_language TEXT,
        #         origin_country TEXT,
        #         production_countries TEXT,
        #         production_companies TEXT,
        #         genres TEXT,
        #         budget BIGINT,
        #         revenue BIGINT,
        #         runtime INTEGER,
        #         overview TEXT,
        #         popularity FLOAT,
        #         vote_average FLOAT,
        #         vote_count INTEGER,
        #         cast_top_10 TEXT,
        #         directors TEXT,
        #         youtube_trailer_key TEXT
        #     );
        # """))

    df.to_sql(
        TABLE_NAME,
        engine,
        schema=DB_SCHEMA,
        if_exists="append",
        index=False,
        method="multi",
        chunksize=1000,
    )


