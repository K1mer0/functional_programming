from __future__ import annotations

import re
from typing import Iterable, Set, Tuple

_WS = re.compile(r"\s+")
_PUNCT = re.compile(r"[^\w\s\-]+", flags=re.UNICODE)


def normalize_text(s: str) -> str:
    s = s.lower().strip()
    s = _PUNCT.sub(" ", s)
    s = _WS.sub(" ", s)
    return s.strip()


def split_csv_like(s: str) -> Tuple[str, ...]:
    # Split by comma or semicolon.
    if not s.strip():
        return ()
    parts = re.split(r"[;,]", s)
    return tuple(p.strip() for p in parts if p.strip())


def to_norm_set(items: Iterable[str]) -> Set[str]:
    return {normalize_text(x) for x in items if normalize_text(x)}


def parse_preferences(genres: str, authors: str, keywords: str) -> dict:
    "Return normalized preferences dict."
    return {
        "genres": to_norm_set(split_csv_like(genres)),
        "authors": to_norm_set(split_csv_like(authors)),
        "keywords": to_norm_set(split_csv_like(keywords)),
    }
