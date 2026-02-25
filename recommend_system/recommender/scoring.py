from __future__ import annotations

from typing import Set, Tuple
from .models import Book
from .preprocess import normalize_text

# Weights (can be tuned)
W_AUTHOR = 5
W_GENRE = 3
W_KEYWORD = 1


def _match_author(book: Book, authors: Set[str]) -> bool:
    if not authors:
        return False
    return normalize_text(book.author) in authors


def _match_genre(book: Book, genres: Set[str]) -> bool:
    if not genres:
        return False
    return normalize_text(book.genre) in genres


def _keyword_hits(book: Book, keywords: Set[str]) -> Tuple[int, Tuple[str, ...]]:
    if not keywords:
        return 0, ()
    hay = normalize_text(f"{book.title} {book.description}")
    hits = tuple(sorted({kw for kw in keywords if kw and kw in hay}))
    return len(hits), hits


def score_book(book: Book, prefs: dict) -> tuple[int, tuple[str, ...]]:
    # Return (score, matched_keywords).
    authors = prefs.get("authors", set())
    genres = prefs.get("genres", set())
    keywords = prefs.get("keywords", set())

    score = 0
    if _match_author(book, authors):
        score += W_AUTHOR
    if _match_genre(book, genres):
        score += W_GENRE

    kcnt, hits = _keyword_hits(book, keywords)
    score += W_KEYWORD * kcnt
    return score, hits
