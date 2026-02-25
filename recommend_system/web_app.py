from __future__ import annotations
from flask import Flask, request, render_template_string, send_file
from pathlib import Path
import io, json, csv

from recommender.io_utils import load_books_json
from recommender.preprocess import parse_preferences
from recommender.pipeline import filter_only_selected_genres, filter_year_min, score_books, sort_recommendations

app = Flask(__name__)

TEMPLATE = """
<!doctype html>
<html lang="ru">
<head>
  <meta charset="utf-8"/>
  <title>Book Recommender</title>
  <style>
    body { font-family: system-ui, sans-serif; margin: 24px; }
    input, select { padding: 6px; width: 100%; }
    .grid { display: grid; grid-template-columns: 1fr 1fr; gap: 12px; }
    table { border-collapse: collapse; width: 100%; margin-top: 16px; }
    th, td { border: 1px solid #ccc; padding: 8px; text-align: left; vertical-align: top; }
    th { background: #f6f6f6; }
    .btn { padding: 8px 12px; margin-top: 12px; margin-right: 8px; }
    .muted { color: #666; }
    .desc { max-width: 680px; }
  </style>
</head>
<body>
  <h2>Рекомендации книг</h2>

  <form method="post">
    <div class="grid">
      <div>
        <label>Жанры (через запятую)</label>
        <input name="genres" value="{{genres}}"/>
      </div>
      <div>
        <label>Авторы (через запятую)</label>
        <input name="authors" value="{{authors}}"/>
      </div>
      <div>
        <label>Ключевые слова (через запятую)</label>
        <input name="keywords" value="{{keywords}}"/>
      </div>
      <div>
        <label>Год от (пусто = без фильтра)</label>
        <input name="year_min" value="{{year_min}}"/>
      </div>
      <div>
        <label>Только указанные жанры</label>
        <select name="only_genres">
          <option value="no" {% if only_genres=='no' %}selected{% endif %}>нет</option>
          <option value="yes" {% if only_genres=='yes' %}selected{% endif %}>да</option>
        </select>
      </div>
      <div>
        <label>Сортировка</label>
        <select name="sort_by">
          <option value="rating" {% if sort_by=='rating' %}selected{% endif %}>по рейтингу</option>
          <option value="title" {% if sort_by=='title' %}selected{% endif %}>по алфавиту</option>
          <option value="year" {% if sort_by=='year' %}selected{% endif %}>по году</option>
        </select>
      </div>
    </div>

    <div>
      <button class="btn" type="submit" name="action" value="recommend">Показать</button>
      {% if rows %}
        <button class="btn" type="submit" name="action" value="to_read">Добавить выбранные в “прочитать”</button>
      {% endif %}
    </div>

    {% if rows %}
      <p class="muted">
        Экспорт:
        <a href="/export?kind=recs&fmt=json">рекомендации JSON</a> |
        <a href="/export?kind=recs&fmt=csv">рекомендации CSV</a>
        {% if to_read %}
          | <a href="/export?kind=to_read&fmt=json">прочитать JSON</a>
          | <a href="/export?kind=to_read&fmt=csv">прочитать CSV</a>
        {% endif %}
      </p>

      <table>
        <tr>
          <th>выбрать</th>
          <th>№</th><th>score</th><th>год</th><th>жанр</th><th>автор</th><th>название</th><th class="desc">описание</th><th>ключевые слова</th>
        </tr>
        {% for r in rows %}
          <tr>
            <td><input type="checkbox" name="pick" value="{{r.rank}}"/></td>
            <td>{{r.rank}}</td>
            <td>{{r.score}}</td>
            <td>{{r.year}}</td>
            <td>{{r.genre}}</td>
            <td>{{r.author}}</td>
            <td>{{r.title}}</td>
            <td class="desc">{{r.description}}</td>
            <td>{{r.matched_keywords}}</td>
          </tr>
        {% endfor %}
      </table>
    {% endif %}

    {% if to_read %}
      <h3>Список “прочитать” ({{to_read|length}})</h3>
      <ul>
        {% for r in to_read[:10] %}
          <li><b>{{r.title}}</b> — {{r.author}} ({{r.year}})</li>
        {% endfor %}
        {% if to_read|length > 10 %}<li class="muted">… и ещё {{to_read|length - 10}}</li>{% endif %}
      </ul>
    {% endif %}
  </form>
</body>
</html>
"""

_LAST_ROWS = []
_TO_READ = []


def _maybe_int(x: str):
    x = (x or "").strip()
    if not x:
        return None
    try:
        return int(x)
    except ValueError:
        return None


@app.route("/", methods=["GET", "POST"])
def index():
    global _LAST_ROWS, _TO_READ

    genres = authors = keywords = ""
    year_min = ""
    only_genres = "no"
    sort_by = "rating"
    rows = _LAST_ROWS
    to_read = _TO_READ

    if request.method == "POST":
        action = (request.form.get("action") or "recommend").lower()

        genres = request.form.get("genres", "")
        authors = request.form.get("authors", "")
        keywords = request.form.get("keywords", "")
        year_min = request.form.get("year_min", "")
        only_genres = request.form.get("only_genres", "no")
        sort_by = request.form.get("sort_by", "rating")

        prefs = parse_preferences(genres, authors, keywords)
        books = load_books_json(Path("data/books.json"))

        if only_genres == "yes":
            books = filter_only_selected_genres(books, prefs["genres"])
        books = filter_year_min(books, _maybe_int(year_min))

        rows = sort_recommendations(score_books(books, prefs), sort_by=sort_by)
        any_pref = any(prefs[k] for k in ("genres", "authors", "keywords"))
        if any_pref:
            rows = [r for r in rows if int(r["score"]) > 0]

        _LAST_ROWS = rows

        if action == "to_read":
            picks = request.form.getlist("pick")
            try:
                pick_ranks = {int(x) for x in picks}
            except ValueError:
                pick_ranks = set()
            _TO_READ = [r for r in _LAST_ROWS if int(r.get("rank", 0)) in pick_ranks]
            to_read = _TO_READ

    return render_template_string(
        TEMPLATE,
        rows=rows,
        to_read=to_read,
        genres=genres,
        authors=authors,
        keywords=keywords,
        year_min=year_min,
        only_genres=only_genres,
        sort_by=sort_by,
    )


@app.route("/export")
def export():
    kind = (request.args.get("kind") or "recs").lower()
    fmt = (request.args.get("fmt") or "json").lower()
    rows = _LAST_ROWS if kind == "recs" else _TO_READ

    if fmt == "csv":
        buf = io.StringIO()
        fieldnames = ["rank", "score", "title", "author", "genre", "year", "description", "matched_keywords"]
        w = csv.DictWriter(buf, fieldnames=fieldnames)
        w.writeheader()
        for r in rows:
            w.writerow({k: r.get(k, "") for k in fieldnames})
        mem = io.BytesIO(buf.getvalue().encode("utf-8"))
        mem.seek(0)
        name = "recommendations.csv" if kind == "recs" else "to_read.csv"
        return send_file(mem, as_attachment=True, download_name=name, mimetype="text/csv")

    mem = io.BytesIO(json.dumps(rows, ensure_ascii=False, indent=2).encode("utf-8"))
    mem.seek(0)
    name = "recommendations.json" if kind == "recs" else "to_read.json"
    return send_file(mem, as_attachment=True, download_name=name, mimetype="application/json")


if __name__ == "__main__":
    app.run(debug=True)
