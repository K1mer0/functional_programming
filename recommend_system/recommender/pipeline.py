from __future__ import annotations

from typing import Iterable, Iterator, List, Dict, Any, Optional

from .models import Book
from .scoring import score_book
from .preprocess import normalize_text


def filter_only_selected_genres(books: Iterable[Book], genres: set[str]) -> Iterator[Book]:
    if not genres:
        yield from books
        return
    norm_genres = {normalize_text(g) for g in genres}
    for b in books:
        if normalize_text(b.genre) in norm_genres:
            yield b


def filter_year_min(books: Iterable[Book], year_min: Optional[int]) -> Iterator[Book]:
    if not year_min:
        yield from books
        return
    for b in books:
        if b.year >= year_min:
            yield b


def score_books(books: Iterable[Book], prefs: dict) -> Iterator[Dict[str, Any]]:
    # Generator: yields dict rows with score + metadata.
    for b in books:
        score, hits = score_book(b, prefs)
        yield {
            "score": score,
            "title": b.title,
            "author": b.author,
            "genre": b.genre,
            "year": b.year,
            "description": b.description,
            "matched_keywords": ", ".join(hits),
        }


def sort_recommendations(rows: Iterable[Dict[str, Any]], sort_by: str = "rating") -> List[Dict[str, Any]]:
    data = list(rows)

    if sort_by == "title":
        key = lambda r: (normalize_text(str(r.get("title", ""))), -int(r.get("score", 0)))
        reverse = False
    elif sort_by == "year":
        key = lambda r: (int(r.get("year", 0)), -int(r.get("score", 0)))
        reverse = True
    else:  # rating
        key = lambda r: (int(r.get("score", 0)), int(r.get("year", 0)))
        reverse = True

    data_sorted = sorted(data, key=key, reverse=reverse)
    return [{**r, "rank": i + 1} for i, r in enumerate(data_sorted)]
