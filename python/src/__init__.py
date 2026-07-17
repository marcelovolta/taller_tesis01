# --- Previous eager star-imports (replaced by defensive loop below) ---
# from .config import *
# from .trailer_comments import *
# from .movie_database import *
# from .box_office import *
# from .sentiment import *
# -----------------------------------------------------------------------

import logging as _logging

# config is a hard requirement — every other module reads from it.
from .config import *

# Each remaining module pulls in its own third-party deps (googleapiclient,
# beautifulsoup4, torch, transformers, ...). Importing them defensively means
# a venv that only has deps for one pipeline (e.g. sentiment on the macOS
# host, with torch+transformers but no YouTube/scraper libs) can still load
# `src` and run that one pipeline.
_logger = _logging.getLogger(__name__)

for _name in ("text_cleaning", "trailer_comments", "movie_database", "box_office", "sentiment"):
    try:
        _mod = __import__(f"src.{_name}", fromlist=["*"])
        _exports = getattr(_mod, "__all__", None) or [
            a for a in dir(_mod) if not a.startswith("_")
        ]
        for _attr in _exports:
            globals()[_attr] = getattr(_mod, _attr)
    except ImportError as _e:
        _logger.warning(f"Optional module 'src.{_name}' not loaded: {_e}")