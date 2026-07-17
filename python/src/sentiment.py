import logging
import os
import datetime
from typing import Iterator

import pandas as pd
from sqlalchemy import create_engine, text

from src import config

logger = logging.getLogger(__name__)

# This module is intended to run from the macOS host (not the dev container)
# so that Apple MPS can be used for inference. It connects to Postgres on the
# host-exposed port (see POSTGRE_HOST_FROM_HOST / POSTGRE_PORT_FROM_HOST in
# config.yaml).
POSTGRE_USER = config.POSTGRE_USER
POSTGRE_PASS = config.POSTGRE_PASS
POSTGRE_HOST = config.POSTGRE_HOST_FROM_HOST
POSTGRE_PORT = config.POSTGRE_PORT_FROM_HOST
DB_NAME = config.DB_NAME
DB_SCHEMA = config.DB_SCHEMA

COMMENTS_TABLE = "trailer_comments"
SENTIMENT_TABLE = "trailer_comments_sentiment"

MODEL_PATH = os.path.expanduser(config.MODEL_PATH)
MODEL_VERSION = os.path.basename(MODEL_PATH.rstrip("/"))

# DistilBERT positional embeddings are sized for 512 tokens.
MAX_LENGTH = 512


# ----
# Database helpers
# ----

def get_engine():
    return create_engine(
        f"postgresql+psycopg2://{POSTGRE_USER}:{POSTGRE_PASS}@{POSTGRE_HOST}:{POSTGRE_PORT}/{DB_NAME}"
    )


def ensure_tables_exist(engine):
    """Create the sentiment table if it doesn't exist yet."""
    with engine.begin() as conn:
        conn.execute(text(f"""
            CREATE TABLE IF NOT EXISTS {DB_SCHEMA}.{SENTIMENT_TABLE} (
                comment_id      TEXT PRIMARY KEY,
                label           TEXT,
                score           REAL,
                scored_at       TIMESTAMPTZ DEFAULT now(),
                model_version   TEXT
            );
        """))
    logger.info("Sentiment table verified/created.")


def reset_sentiment(engine):
    """Clears every previously scored row so the model can be re-run."""
    with engine.begin() as conn:
        conn.execute(text(f"TRUNCATE TABLE {DB_SCHEMA}.{SENTIMENT_TABLE};"))
    logger.info("Sentiment table cleared. Ready to re-score from scratch.")


def count_pending(engine) -> int:
    query = text(f"""
        SELECT COUNT(*)
        FROM {DB_SCHEMA}.{COMMENTS_TABLE} c
        LEFT JOIN {DB_SCHEMA}.{SENTIMENT_TABLE} s ON c.comment_id = s.comment_id
        WHERE s.comment_id IS NULL
          AND c.clean_text IS NOT NULL
          AND length(btrim(c.clean_text)) > 0
    """)
    with engine.connect() as conn:
        return int(conn.execute(query).scalar() or 0)


def iter_pending_comments(engine, chunk_size: int) -> Iterator[pd.DataFrame]:
    """
    Yields chunks of (comment_id, clean_text) for comments that haven't been scored
    yet. Uses an anti-join against the sentiment table so a re-run picks up
    only what's left.
    """
    query = text(f"""
        SELECT c.comment_id, c.clean_text
        FROM {DB_SCHEMA}.{COMMENTS_TABLE} c
        LEFT JOIN {DB_SCHEMA}.{SENTIMENT_TABLE} s ON c.comment_id = s.comment_id
        WHERE s.comment_id IS NULL
          AND c.clean_text IS NOT NULL
          AND length(btrim(c.clean_text)) > 0
        ORDER BY c.comment_id
    """)
    with engine.connect().execution_options(stream_results=True) as conn:
        for chunk in pd.read_sql(query, conn, chunksize=chunk_size):
            yield chunk


def save_sentiments(engine, rows: list[dict]) -> int:
    """Upsert-style insert; existing rows are kept untouched."""
    if not rows:
        return 0
    with engine.begin() as conn:
        conn.execute(text(f"""
            INSERT INTO {DB_SCHEMA}.{SENTIMENT_TABLE}
                (comment_id, label, score, scored_at, model_version)
            VALUES
                (:comment_id, :label, :score, now(), :model_version)
            ON CONFLICT (comment_id) DO NOTHING
        """), rows)
    return len(rows)


# ----
# Model helpers
# ----

def auto_device() -> str:
    """Pick the best available device: MPS (Apple Silicon) → CUDA → CPU."""
    import torch
    if torch.backends.mps.is_available() and torch.backends.mps.is_built():
        return "mps"
    if torch.cuda.is_available():
        return "cuda"
    return "cpu"


def load_model(device: str):
    """Load the fine-tuned DistilBERT classifier and its tokenizer."""
    from transformers import AutoTokenizer, AutoModelForSequenceClassification
    import torch

    logger.info(f"Loading model from {MODEL_PATH} onto device={device}.")
    tokenizer = AutoTokenizer.from_pretrained(MODEL_PATH)
    model = AutoModelForSequenceClassification.from_pretrained(MODEL_PATH)
    model.to(device)
    model.eval()
    return tokenizer, model


def score_texts(texts: list[str], tokenizer, model, device: str) -> list[tuple[str, float]]:
    """Run a single forward pass on a batch and return (label, score) per row."""
    import torch

    enc = tokenizer(
        texts,
        padding=True,
        truncation=True,
        max_length=MAX_LENGTH,
        return_tensors="pt",
    )
    enc = {k: v.to(device) for k, v in enc.items()}

    with torch.no_grad():
        logits = model(**enc).logits
    probs = torch.softmax(logits, dim=-1)
    confs, preds = probs.max(dim=-1)

    id2label = model.config.id2label
    return [
        (id2label[int(p.item())], float(c.item()))
        for p, c in zip(preds, confs)
    ]


# ----
# Main orchestration
# ----

def process_trailer_sentiment(
    reset: bool = False,
    fetch_chunk_size: int = 1000,
    batch_size: int = 32,
):
    """
    Scores every row in `trailer_comments` with the fine-tuned DistilBERT
    classifier and writes results to `trailer_comments_sentiment`.

    Args:
        reset: If True, truncates the sentiment table and re-scores everything.
        fetch_chunk_size: How many rows to pull from Postgres at a time.
        batch_size: How many rows per forward pass through the model.

    Resumable: re-running picks up only comments not yet present in
    `trailer_comments_sentiment`.
    """
    engine = get_engine()
    ensure_tables_exist(engine)

    if reset:
        logger.info("Reset requested — clearing sentiment table.")
        reset_sentiment(engine)

    pending = count_pending(engine)
    logger.info(f"{pending} comments pending sentiment scoring.")
    if pending == 0:
        logger.info("Nothing to score. All done.")
        return

    device = auto_device()
    tokenizer, model = load_model(device)

    total_scored = 0
    start = datetime.datetime.now()

    for chunk in iter_pending_comments(engine, fetch_chunk_size):
        chunk_rows: list[dict] = []
        ids = chunk["comment_id"].tolist()
        texts = [str(t) for t in chunk["clean_text"].tolist()]

        for i in range(0, len(texts), batch_size):
            batch_ids = ids[i : i + batch_size]
            batch_texts = texts[i : i + batch_size]
            try:
                results = score_texts(batch_texts, tokenizer, model, device)
            except Exception as e:
                logger.error(f"Inference failed on batch of {len(batch_texts)}: {e}")
                continue

            for cid, (label, score) in zip(batch_ids, results):
                chunk_rows.append({
                    "comment_id": cid,
                    "label": label,
                    "score": score,
                    "model_version": MODEL_VERSION,
                })

        saved = save_sentiments(engine, chunk_rows)
        total_scored += saved
        elapsed = (datetime.datetime.now() - start).total_seconds()
        rate = total_scored / elapsed if elapsed > 0 else 0.0
        logger.info(
            f"Saved {saved} (cumulative {total_scored}/{pending}) — {rate:.1f} rows/s on {device}."
        )

    logger.info(f"Sentiment scoring complete. {total_scored} rows scored.")
