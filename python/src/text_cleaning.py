import csv
import logging

logger = logging.getLogger(__name__)

_emoji_map: dict | None = None


def _load(path: str) -> dict:
    result = {}
    with open(path, newline="", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            if row.get("replacement"):
                result[row["emoji"]] = row["replacement"]
    logger.info(f"Loaded {len(result)} emoji replacements from {path}")
    return result


def get_emoji_map(path: str) -> dict:
    """Returns the emoji→replacement dict, loading from path on first call."""
    global _emoji_map
    if _emoji_map is None:
        _emoji_map = _load(path)
    return _emoji_map


def replace_emojis(text: str, emoji_map: dict) -> str:
    if not text:
        return text
    for emoji, replacement in emoji_map.items():
        text = text.replace(emoji, f" {replacement} ")
    return text
