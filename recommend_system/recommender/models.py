from __future__ import annotations

from dataclasses import dataclass
from typing import Mapping, Any


@dataclass(frozen=True, slots=True)
class Book:
    title: str
    author: str
    genre: str
    description: str
    year: int

    @staticmethod
    def from_mapping(m: Mapping[str, Any]) -> "Book":
        title = str(m.get("title", "")).strip()
        author = str(m.get("author", "")).strip()
        genre = str(m.get("genre", "")).strip()
        description = str(m.get("description", "")).strip()
        year_raw = m.get("year", 0)
        try:
            year = int(year_raw)
        except (TypeError, ValueError):
            year = 0
        return Book(title=title, author=author, genre=genre, description=description, year=year)
