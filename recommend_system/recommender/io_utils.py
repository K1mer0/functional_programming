from __future__ import annotations

import csv
import json
from pathlib import Path
from typing import Iterable, Iterator, Dict, Any, List

from .models import Book


def load_books_json(path: str | Path) -> Iterator[Book]:
    # Generator: yields Book from a JSON array file.
    p = Path(path)
    with p.open("r", encoding="utf-8") as f:
        data = json.load(f)
    if not isinstance(data, list):
        raise ValueError("books JSON must be a list of objects")
    for item in data:
        if isinstance(item, dict):
            yield Book.from_mapping(item)


def save_recommendations_json(path: str | Path, rows: Iterable[Dict[str, Any]]) -> None:
    p = Path(path)
    p.parent.mkdir(parents=True, exist_ok=True)
    with p.open("w", encoding="utf-8") as f:
        json.dump(list(rows), f, ensure_ascii=False, indent=2)


def save_recommendations_csv(path: str | Path, rows: Iterable[Dict[str, Any]]) -> None:
    p = Path(path)
    p.parent.mkdir(parents=True, exist_ok=True)
    rows_list: List[Dict[str, Any]] = list(rows)
    fieldnames = ["rank", "score", "title", "author", "genre", "year", "description", "matched_keywords"]
    with p.open("w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        for r in rows_list:
            w.writerow({k: r.get(k, "") for k in fieldnames})
