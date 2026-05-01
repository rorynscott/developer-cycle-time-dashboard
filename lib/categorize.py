"""
Shared text-categorization helper used across data sources.

Categories are defined once at the top level of config.toml and applied at
query time (not ETL time) — so updating patterns reclassifies everything
immediately. Matching is first-match-wins in config order; unmatched text
falls into "Uncategorized".
"""

import re
from functools import lru_cache

from lib.config import get_categories


@lru_cache(maxsize=1)
def _compiled_categories():
    """Compile patterns once per process."""
    compiled = []
    for cat in get_categories():
        pats = [re.compile(p, re.IGNORECASE) for p in cat["patterns"]]
        compiled.append((cat["name"], pats))
    return compiled


def categorize(text):
    """Return the first matching category name, or 'Uncategorized'."""
    if not text:
        return "Uncategorized"
    for name, pats in _compiled_categories():
        for pat in pats:
            if pat.search(text):
                return name
    return "Uncategorized"


def category_names():
    """Return the ordered list of category names plus 'Uncategorized' at the end."""
    return [c["name"] for c in get_categories()] + ["Uncategorized"]
