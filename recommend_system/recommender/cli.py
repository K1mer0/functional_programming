from __future__ import annotations

import sys
from pathlib import Path
from typing import Optional, List, Dict, Any

from .io_utils import load_books_json, save_recommendations_json, save_recommendations_csv
from .preprocess import parse_preferences
from .pipeline import (
    filter_only_selected_genres,
    filter_year_min,
    score_books,
    sort_recommendations,
)


def _prompt(msg: str) -> str:
    try:
        return input(msg)
    except EOFError:
        return ""


def _parse_int(s: str) -> Optional[int]:
    s = s.strip()
    if not s:
        return None
    try:
        return int(s)
    except ValueError:
        return None


def _clip(s: str, n: int) -> str:
    s = (s or "").strip().replace("\n", " ")
    return s if len(s) <= n else s[: max(0, n - 1)] + "…"


def _print_table(rows: List[Dict[str, Any]], top_n: int = 10) -> None:
    if not rows:
        print("\nНет рекомендаций (проверьте фильтры или предпочтения).")
        return
    shown = rows[:top_n]
    print("\nРекомендации:")
    print("-" * 110)
    print(f"{'№':>2}  {'score':>5}  {'год':>4}  {'жанр':<18}  {'автор':<22}  {'название':<28}  {'описание'}")
    print("-" * 110)
    for r in shown:
        print(
            f"{r['rank']:>2}  {r['score']:>5}  {r['year']:>4}  "
            f"{_clip(r['genre'],18):<18}  {_clip(r['author'],22):<22}  {_clip(r['title'],28):<28}  "
            f"{_clip(r.get('description',''), 60)}"
        )
    print("-" * 110)
    if len(rows) > top_n:
        print(f"Показано {top_n} из {len(rows)}. Можно сохранить полный список в файл.")


def _select_to_read(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    if not rows:
        return []
    raw = _prompt("Введите номера понравившихся книг через запятую (или Enter чтобы пропустить): ")
    raw = raw.strip()
    if not raw:
        return []
    try:
        nums = [int(x.strip()) for x in raw.replace(";", ",").split(",") if x.strip()]
    except ValueError:
        print("Некорректный ввод: ожидались числа.")
        return []
    chosen = {n for n in nums if 1 <= n <= len(rows)}
    return [r for r in rows if r["rank"] in chosen]


def run_cli(data_path: str | Path = "data/books.json") -> int:
    data_path = Path(data_path)
    if not data_path.exists():
        print(f"Файл базы книг не найден: {data_path}")
        return 2

    print("=== Рекомендательная система книг (CLI) ===")
    genres = _prompt("Любимые жанры (через запятую): ")
    authors = _prompt("Любимые авторы (через запятую): ")
    keywords = _prompt("Ключевые слова (через запятую): ")
    prefs = parse_preferences(genres, authors, keywords)

    only_selected = _prompt("Фильтр: только указанные жанры? (y/N): ").strip().lower() == "y"
    year_min = _parse_int(_prompt("Фильтр: книги не раньше какого года? (Enter = без фильтра): "))

    sort_by = _prompt("Сортировка: rating/title/year (Enter=rating): ").strip().lower() or "rating"
    if sort_by not in {"rating", "title", "year"}:
        print("Неизвестный вариант сортировки, будет rating.")
        sort_by = "rating"

    top_n = _parse_int(_prompt("Сколько показать в консоли? (Enter=10): ")) or 10

    books = load_books_json(data_path)
    if only_selected:
        books = filter_only_selected_genres(books, prefs["genres"])
    books = filter_year_min(books, year_min)

    rows = score_books(books, prefs)
    rows_sorted = sort_recommendations(rows, sort_by=sort_by)

    any_pref = any(prefs[k] for k in ("genres", "authors", "keywords"))
    if any_pref:
        rows_sorted = [r for r in rows_sorted if int(r["score"]) > 0]

    _print_table(rows_sorted, top_n=top_n)

    to_read = _select_to_read(rows_sorted)
    if to_read:
        out = Path("out")
        out.mkdir(exist_ok=True)
        save_recommendations_json(out / "to_read.json", to_read)
        print(f"Список 'прочитать' сохранён: {out/'to_read.json'}")

    save = _prompt("Сохранить рекомендации в файл? (json/csv/нет): ").strip().lower()
    if save in {"json", "csv"}:
        out = Path("out")
        out.mkdir(exist_ok=True)
        if save == "json":
            save_recommendations_json(out / "recommendations.json", rows_sorted)
            print(f"Сохранено: {out/'recommendations.json'}")
        else:
            save_recommendations_csv(out / "recommendations.csv", rows_sorted)
            print(f"Сохранено: {out/'recommendations.csv'}")

    print("Готово.")
    return 0


if __name__ == "__main__":
    sys.exit(run_cli())
